;;;
;;; executor.scm - executor services for threaded programs
;;;  
;;;   Copyright (c) 2008  Rui Ueyama  <rui314@gmail.com>
;;;   
;;;   Redistribution and use in source and binary forms, with or without
;;;   modification, are permitted provided that the following conditions
;;;   are met:
;;;   
;;;   1. Redistributions of source code must retain the above copyright
;;;      notice, this list of conditions and the following disclaimer.
;;;  
;;;   2. Redistributions in binary form must reproduce the above copyright
;;;      notice, this list of conditions and the following disclaimer in the
;;;      documentation and/or other materials provided with the distribution.
;;;  
;;;   3. Neither the name of the authors nor the names of its contributors
;;;      may be used to endorse or promote products derived from this
;;;      software without specific prior written permission.
;;;  
;;;   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;;;   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;;;   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;;;   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;;;   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;;;   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
;;;   TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
;;;   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
;;;   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;;   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;;   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;  

(define-module concurrent.executor
  (export <thread-pool-executor>
          executor-submit
          executor-shutdown!
          executor-await-termination
          <completion-service>
          completion-service-submit
          completion-service-get
          )
  (use gauche.mop.validator)
  (use gauche.threads)
  (use concurrent.blocking-queue)
  (use concurrent.future)
  (use concurrent.util)
  )
(select-module concurrent.executor)

;;;
;;; Thread-pool implementation
;;;
;;; The model of this thread pool class is Java's ThreadPoolExecutor.
;;;

(define (%core-pool-size-validator executor size)
  (define (change-pool-size)
    (with-locking-mutex (ref executor 'mutex)
      (lambda ()
        (let1 nthread (- (ref executor 'core-pool-size) size)
          (if (negative? nthread)
            (%queue-poison executor (- nthread))
            (let1 new-workers (min (blocking-queue-length (ref executor 'work-queue))
                                   nthread)
              (dotimes (_ new-workers)
                (%create-worker executor))))))))
  (and (or (not (integer? size))
           (< size 0))
       (error "positive integer required, but got" size))
  (and (slot-bound? executor 'core-pool-size)
       (let1 max-size (ref executor 'maximum-pool-size)
         (and max-size
              (< max-size size)
              (error "core-pool-size is greater than maximum-pool-size"))
         (change-pool-size)))
  size)             

(define (%maximum-pool-size-validator executor size)
  (when (and (slot-bound? executor 'maximum-pool-size)
             size
             (< size (ref executor 'core-pool-size)))
    (error "maximum-pool-size is less than core-pool-size"))
  size)

(define (%keep-alive-time-validator executor time)
  (and time
       (not (time? time))
       (not (real? time))
       (error "<time> or real required, but got" time))
  time)

(define-class <thread-pool-executor> ()
  ((core-pool-size     :init-keyword :core-pool-size
                       :validator %core-pool-size-validator)
   (maximum-pool-size  :init-keyword :maximum-pool-size
                       :validator %maximum-pool-size-validator)
   (keep-alive-time    :init-keyword :keep-alive-time
                       :validator %keep-alive-time-validator)
   (work-queue         :init-keyword :work-queue
                       :init-form (make-blocking-queue))
   (rejected-execution-handler :init-keyword :rejected-execution-handler)
   (current-pool-size  :init-value 0)
   (%state             :init-value 'running) ;; running, terminating, or terminated
   (%mutex             :init-form (make-mutex))
   (%await-termination :init-form (make-condition-variable))
   )
  :metaclass <validator-meta>)

(define *poison* (cons #f #f))

(define (%queue-poison executor n)
  (dotimes (_ n)
    (blocking-queue-enqueue! (ref executor 'work-queue) *poison*)))

(define (%create-worker executor)
  (define (cleanup-worker)
    (with-locking-mutex (ref executor '%mutex)
      (lambda ()
        (dec! (ref executor 'current-pool-size))
        (when (zero? (ref executor 'current-pool-size))
          (set! (ref executor '%state) 'terminated))
        (condition-variable-broadcast! (ref executor '%await-termination)))))
  (define (main-loop)
    (let1 future
        (if (slot-bound? executor 'keep-alive-time)
          (blocking-queue-dequeue! (ref executor 'work-queue) :timeout (ref executor 'keep-alive-time))
          (blocking-queue-dequeue! (ref executor 'work-queue)))
      (if (or (not future) (eq? future *poison*))
        (cleanup-worker)
        (begin (future-run! future)
               (main-loop)))))
  (thread-start! (make-thread main-loop)))

(define-method executor-submit ((executor <thread-pool-executor>) task)
  (define (has-room?)
    (let1 cap (blocking-queue-remaining-capacity (ref executor 'work-queue))
      (or (not cap)
          (not (zero? cap)))))
  (define (enqueue!)
    (let1 future (if (future? task)
                   task
                   (make-future task))
      (blocking-queue-enqueue! (ref executor 'work-queue) future)
      future))
  (define (reject)
    (when (slot-bound? executor 'rejected-execution-handler)
      (let1 handler (ref executor 'rejected-execution-handler)
        (handler task executor))))
  (with-locking-mutex (ref executor '%mutex)
    (lambda ()
      (receive (should-create-worker? should-queue?)
          (cond ((not (eq? (ref executor '%state) 'running))
                 (values #f #f))
                ((< (ref executor 'current-pool-size) (ref executor 'core-pool-size))
                 (values #t #t))
                ((or (not (slot-bound? executor 'maximum-pool-size))
                     (= (ref executor 'current-pool-size) (ref executor 'maximum-pool-size)))
                 (values #f (has-room?)))
                ((has-room?)
                 (values #t #t))
                (else
                 (values #t #t)))
        (when should-create-worker?
          (%create-worker executor)
          (inc! (ref executor 'current-pool-size)))
        (if should-queue?
          (enqueue!)
          (reject))))))

(define-method executor-shutdown! ((executor <thread-pool-executor>))
  (with-locking-mutex (ref executor '%mutex)
    (lambda ()
      (when (eq? (ref executor '%state) 'running)
        (set! (ref executor '%state) 'terminating)
        (%queue-poison executor (ref executor 'current-pool-size))))))

(define-method executor-await-termination ((executor <thread-pool-executor>) . timeout)
  (define abs-timeout (->absolute-time (get-optional timeout #f)))
  (let1 mutex (ref executor '%mutex)
    (with-locking-mutex mutex
      (rec (loop)
        (cond ((and (not (eq? (ref executor '%state) 'running))
                    (zero? (ref executor 'current-pool-size)))
               #t)
              ((if abs-timeout
                 (mutex-unlock! mutex (ref executor '%await-termination) abs-timeout)
                 (mutex-unlock! mutex (ref executor '%await-termination)))
               (mutex-lock! mutex)
               (loop))
              (else #f))))))

;;;
;;; Completion service
;;;

(define-class <completion-service> ()
  ((executor :init-keyword :executor)
   (queue    :init-form (make-blocking-queue))))

(define (completion-service-submit service task)
  (let1 future
      (make-future
       task
       :completion-hook (cut blocking-queue-enqueue! (ref service 'queue) <>))
    (executor-submit (ref service 'executor) future)))

(define (completion-service-get service . args)
  (apply blocking-queue-dequeue! (ref service 'queue) args))

(provide "concurrent/executor")
