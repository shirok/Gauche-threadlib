;;;
;;; latch.scm - a synchronization point at where multiple threads wait
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

;; Countdown latch is a synchronization point at where multiple
;; threads wait, until the counter in the latch becomes 0.  The
;; initial value of the counter is set at the time when the latch
;; object is created, and decremented by 1 with 'latch-dec!'.  While
;; the counter is greater than 0, a thread calling 'latch-await' is
;; blocked.  Once the counter becomes 0, all awaited threads are
;; resumed simultaneously, and subsequent call of 'latch-await' never
;; block.
;;
;; Latch object is not reusable; you cannot reset the counter of the
;; latch.

(define-module concurrent.latch
  (export make-count-down-latch
          latch-dec!
          latch-await
          )
  (use gauche.threads)
  )
(select-module concurrent.latch)

(define-class <count-down-latch> ()
  ((mutex   :init-form (make-mutex))
   (condvar :init-form (make-condition-variable))
   (count   :init-keyword :count)))

(define (make-count-down-latch count)
  (when (or (not (integer? count))
            (< count 0))
    (error "positive integer required, but got" count))
  (make <count-down-latch> :count count))

(define-method latch-dec! ((latch <count-down-latch>))
  (let1 mutex (ref latch 'mutex)
    (mutex-lock! mutex)
    (if (zero? (ref latch 'count))
      (mutex-unlock! mutex)
      (begin
        (dec! (ref latch 'count))
        (let1 count (ref latch 'count)
          (when (zero? count)
            (condition-variable-broadcast! (ref latch 'condvar)))
          (mutex-unlock! mutex))))))

(define-method latch-await ((latch <count-down-latch>))
  (let loop ()
    (mutex-lock! (ref latch 'mutex))
    (if (zero? (ref latch 'count))
      (mutex-unlock! (ref latch 'mutex))
      (begin (mutex-unlock! (ref latch 'mutex) (ref latch 'condvar))
             (loop)))))

(provide "concurrent/latch")
