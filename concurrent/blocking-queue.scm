;;;
;;; blocking-queue.scm - blocking queue implementation
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

;; This is an implementation of blocking queue, which is useful for
;; producer-consumer pattern in multi-threaded program.

(define-module concurrent.blocking-queue
  (export make-blocking-queue blocking-queue? blocking-queue-empty?
          blocking-queue-push! blocking-queue-enqueue!
          blocking-queue-pop! blocking-queue-dequeue!
          blocking-queue-dequeue-all!
          blocking-queue-front blocking-queue-rear
          blocking-queue-length
          blocking-queue-remaining-capacity)
  (use gauche.threads)
  (use util.queue)
  (use srfi-19) ; add-duration
  (use concurrent.util)
  )
(select-module concurrent.blocking-queue)

(define-class <blocking-queue> ()
  ((mutex           :init-form (make-mutex))
   (full-condition  :init-form (make-condition-variable))
   (empty-condition :init-form (make-condition-variable))
   (queue           :init-form (make-queue))
   (length          :init-value 0)
   (capacity        :init-value #f :init-keyword :capacity)))

(define (make-blocking-queue . opt)
  (when (eq? (gauche-thread-type) 'none)
    (error "thread is not supported"))
  (make <blocking-queue> :capacity (get-optional opt #f)))

(define (blocking-queue? obj)
  (is-a? obj <blocking-queue>))

(define (%ensure-type obj)
  (unless (blocking-queue? obj)
    (error "blocking-queue required, but got " obj)))

;; #t if we can push one more object to the queue.  Queue must be
;; locked.
(define (%space-available? q)
  (or (not (ref q 'capacity))
      (< (ref q 'length) (ref q 'capacity))))

(define (%queue-has-item? q)
  (not (queue-empty? (ref q 'queue))))

(define (blocking-queue-empty? q)
  (%ensure-type q)
  (with-locking-mutex (ref q 'mutex)
    (lambda ()
      (queue-empty? (ref q 'queue)))))

(define (%queue-add q add! obj arg)
  (define absolute-time
    (if (null? arg)
      #f
      (->absolute-time (car arg))))
  (define (rec)
    (mutex-lock! (ref q 'mutex))
    (if (%space-available? q)
      (begin
        (add! (ref q 'queue) obj)
        (inc! (ref q 'length))
        (condition-variable-broadcast! (ref q 'empty-condition))
        (mutex-unlock! (ref q 'mutex))
        #t)
      (and (mutex-unlock! (ref q 'mutex) (ref q 'full-condition) absolute-time)
           (rec))))
  (%ensure-type q)
  (rec))

(define (blocking-queue-push! q obj . timeout)
  (%queue-add q queue-push! obj timeout))

(define (blocking-queue-enqueue! q obj . timeout)
  (%queue-add q enqueue! obj timeout))

(define (%queue-peek q peek timeout default)
  (define absolute-time (->absolute-time timeout))
  (define (rec)
    (mutex-lock! (ref q 'mutex))
    (if (%queue-has-item? q)
      (begin0
        (peek (ref q 'queue))
        (mutex-unlock! (ref q 'mutex)))
      (if (mutex-unlock! (ref q 'mutex) (ref q 'empty-condition) absolute-time)
        (rec)
        default)))
  (%ensure-type q)
  (rec))

(define (blocking-queue-front q . opts)
  (let-optionals* opts ((timeout #f) (default #f))
    (%queue-peek q queue-front timeout default)))

(define (blocking-queue-rear q . opts)
  (let-optionals* opts ((timeout #f) (default #f))
    (%queue-peek q queue-rear timeout default)))

(define (blocking-queue-dequeue! q . opts)
  (%ensure-type q)
  (let-optionals* opts ((timeout #f) (default #f))
    (define absolute-time (->absolute-time timeout))
    (define (rec)
      (mutex-lock! (ref q 'mutex))
      (if (%queue-has-item? q)
        (begin0
         (dequeue! (ref q 'queue))
         (dec! (ref q 'length))
         (condition-variable-broadcast! (ref q 'full-condition))
         (mutex-unlock! (ref q 'mutex)))
        (if (mutex-unlock! (ref q 'mutex) (ref q 'empty-condition) absolute-time)
          (rec)
          default)))
    (rec)))

(define blocking-queue-pop! blocking-queue-dequeue!)

(define (blocking-queue-dequeue-all! q)
  (%ensure-type q)
  (with-locking-mutex (ref q 'mutex)
    (lambda ()
      (set! (ref q 'length) 0)
      (condition-variable-broadcast! (ref q 'full-condition))
      (dequeue-all! (ref q 'queue)))))

(define (blocking-queue-length q)
  (%ensure-type q)
  (with-locking-mutex (ref q 'mutex)
    (lambda () (ref q 'length))))

(define (blocking-queue-remaining-capacity q)
  (%ensure-type q)
  (and (ref q 'capacity)
       (with-locking-mutex (ref q 'mutex)
         (lambda ()
           (- (ref q 'capacity) (ref q 'length))))))

(provide "concurrent/blocking-queue")
