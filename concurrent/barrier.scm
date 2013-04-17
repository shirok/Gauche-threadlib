;;;
;;; barrier.scm - a synchronization point at where multiple threads wait
;;;  
;;;   Copyright (c) 2009  Rui Ueyama  <rui314@gmail.com>
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

;; Cyclic barrier is a synchronization point at where multiple threads
;; wait, until the number of waiting threads will reach to the preset
;; number.  

(define-module concurrent.barrier
  (export make-cyclic-barrier
          barrier-await
          )
  (use srfi-1)
  (use gauche.threads)
  )
(select-module concurrent.barrier)

(define-class <cyclic-barrier> ()
  ((mutex     :init-form (make-mutex))
   (condvar   :init-form (make-condition-variable))
   (count     :init-keyword :count)
   (nwaiting  :init-value 0)
   (current-parties  :init-value '())
   (former-parties   :init-value '())
   (barrier-action   :init-keyword :barrier-action)))

(define (make-cyclic-barrier count . opts)
  (when (or (not (integer? count))
            (< count 0))
    (error "positive integer required, but got" count))
  (let1 barrier-action (get-optional opts #f)
    (make <cyclic-barrier> :count count :barrier-action barrier-action)))

(define-method barrier-await ((barrier <cyclic-barrier>))
  (mutex-lock! (ref barrier 'mutex))
  (inc! (ref barrier 'nwaiting))
  (cond ((<= (ref barrier 'count) (ref barrier 'nwaiting))
         (set! (ref barrier 'nwaiting) 0)
         (set! (ref barrier 'former-parties)
               (lset-union eq?
                           (ref barrier 'former-parties)
                           (ref barrier 'current-parties)))
         (set! (ref barrier 'current-parties) '())
         (condition-variable-broadcast! (ref barrier 'condvar))
         (mutex-unlock! (ref barrier 'mutex))
         (and-let* ((action (ref barrier 'barrier-action)))
           (action)))
        (else
         (push! (ref barrier 'current-parties) (current-thread))
         (mutex-unlock! (ref barrier 'mutex) (ref barrier 'condvar))
         (let loop ()
           (mutex-lock! (ref barrier 'mutex))
           (cond ((memq (current-thread) (ref barrier 'former-parties))
                  (set! (ref barrier 'former-parties)
                        (delete! (current-thread) (ref barrier 'former-parties) eq?))
                  (mutex-unlock! (ref barrier 'mutex)))
                 (else
                  (mutex-unlock! (ref barrier 'mutex) (ref barrier 'condvar))
                  (loop)))))))

(provide "concurrent/latch")
