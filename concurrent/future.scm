;;;
;;; future.scm - a class representing an asynchronous computation
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

;; Future is a proxy object for an actual value, which may be being computed
;; in another thread.  Future is the basic building block of high-level thread
;; library.  It simplifies thread synchronization and data passing between
;; threads.
;;
;; 'make-future' takes a thunk and returns a future object which wraps the
;; thunk.  The thunk is allowed to be executed by another thread, depending on
;; the execution policy, so that the current thread can do anything other than
;; waiting for the value of the future object.  'future-get' will block if the
;; computation of the thunk is not completed.  Subsequent call of 'future-get'
;; will returns the same object immediately.
;;
;; If the thunk raises an condition, 'future-get' will raise the same
;; condition, to notify the error to the consumer thread.
;;
;; Future is cancellable if the execution of the thunk is not started.
;; 'future-get' to the cancelled future will raise a
;; <future-cancelled-condition> object.  Unfortunately, unlike Java's Future,
;; Gauche's one is not cancellable once the execution started, since Gauche
;; have no reliable way to interrupt the execution of another thread.

(define-module concurrent.future
  (export make-future future? future-run!
          future-get future-cancel! future-done?
          <future-cancelled-condition>)
  (use gauche.threads)
  (use concurrent.util)
  )
(select-module concurrent.future)

;; Possible state of the <future> object is init, running, completed, and
;; error.
(define-class <future> ()
  ((task            :init-keyword :task)
   (completion-hook :init-keyword :completion-hook :init-value #f)
   (mutex           :init-form (make-mutex))
   (condvar         :init-form (make-condition-variable))
   (state           :init-value 'init)
   (result)))

(define-condition-type <future-cancelled-condition> <error> #f)

(define (make-future task . opts)
  (when (eq? (gauche-thread-type) 'none)
    (error "thread is not supported"))
  (let-keywords* opts ((completion-hook #f))
    (make <future>
      :task task
      :completion-hook completion-hook)))

(define (future? obj)
  (is-a? obj <future>))

(define (%ensure-future obj)
  (unless (future? obj)
    (error "blocking-queue required, but got " obj)))

(define (%future-set! future pre-condition value state)
  (with-locking-mutex (ref future 'mutex)
    (lambda ()
      (when (pre-condition)
        (set! (ref future 'result) value)
        (set! (ref future 'state) state)
        (condition-variable-broadcast! (ref future 'condvar))))))

(define (%call-completion-hook future)
  (and-let* ((hook (ref future 'completion-hook)))
    (hook future)))

(define (%future-set-result! future result)
  (define (pre-condition)
    (not (eq? (ref future 'state) 'completed)))
  (%future-set! future pre-condition result 'completed))

(define (%future-set-error! future error)
  (define (pre-condition)
    (not (eq? (ref future 'state) 'completed)))
  (%future-set! future pre-condition error 'error))

(define (future-run! future)
  (%ensure-future future)
  (mutex-lock! (ref future 'mutex))
  (if (eq? (ref future 'state) 'init)
    (begin
      (set! (ref future 'state) 'running)
      (mutex-unlock! (ref future 'mutex))
      (guard (e (else (%future-set-error! future e)))
        (%future-set-result! future ((ref future 'task))))
      (%call-completion-hook future))
    (mutex-unlock! (ref future 'mutex)))
  (undefined))

(define (future-get future . opt)
  (%ensure-future future)
  (let-optionals* opt ((timeout #f) (default #f))
    (define absolute-time (->absolute-time timeout))
    (define (rec)
      (mutex-lock! (ref future 'mutex))
      (cond ((eq? (ref future 'state) 'completed)
             (mutex-unlock! (ref future 'mutex))
             (ref future 'result))
            ((eq? (ref future 'state) 'error)
             (mutex-unlock! (ref future 'mutex))
             (raise (ref future 'result)))
            ((mutex-unlock! (ref future 'mutex) (ref future 'condvar) absolute-time)
             (rec))
            (else
             default)))
    (rec)))
  
(define (future-cancel! future)
  (%ensure-future future)
  (let1 error
      (make-condition <future-cancelled-condition>
        'message "future cancelled")
    (%future-set-error! future error)
    (%call-completion-hook future))
  (undefined))

(define (future-done? future)
  (%ensure-future future)
  (with-locking-mutex (ref future 'mutex)
    (lambda ()
      (case (ref future 'state)
        ((completed error) #t)
        (else #f)))))

(provide "concurrent/future")
