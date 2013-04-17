;;
;; test util.concurrent modules
;;

(use gauche.test)
(test-start "concurrent")

(use srfi-1)
(use gauche.threads)

;;-----------------------------------------------
(test-section "concurrent.blocking-queue")
(use concurrent.blocking-queue)
(test-module 'concurrent.blocking-queue)

(unless (eq? (gauche-thread-type) 'none)
  (test* "blocking-queue?" #f
	 (blocking-queue? 'foo))
  (test* "blocking-queue?" #t
	 (blocking-queue? (make-blocking-queue)))

  (test* "blocking-queue-empty?" #t
	 (blocking-queue-empty? (make-blocking-queue)))
  (test* "blocking-queue-empty?" #f
	 (let1 q (make-blocking-queue)
	   (blocking-queue-push! q 'foo)
	   (blocking-queue-empty? q)))
  (test* "blocking-queue-empty?" #t
	 (let1 q (make-blocking-queue)
	   (blocking-queue-push! q 'foo)
	   (blocking-queue-pop! q)
	   (blocking-queue-empty? q)))

  (let ()
    (define (make-queue)
      (let1 q (make-blocking-queue)
	(blocking-queue-enqueue! q 'foo)
	(blocking-queue-enqueue! q 'bar)
	q))

    (test* "blocking-queue-dequeue!" 'foo
	   (blocking-queue-dequeue! (make-queue)))
    (test* "blocking-queue-pop!" 'foo
	   (blocking-queue-pop! (make-queue)))
    (test* "blocking-queue-dequeue-all!" '(foo bar)
	   (blocking-queue-dequeue-all! (make-queue)))
    (test* "blocking-queue-dequeue-all!" #t
	   (let1 q (make-queue)
	     (blocking-queue-dequeue-all! q)
	     (blocking-queue-empty? q)))
    (test* "blocking-queue-front" '(foo foo)
	   (let1 q (make-queue)
	     (list (blocking-queue-front q)
		   (blocking-queue-front q))))
    (test* "blocking-queue-rear" '(bar bar)
	   (let1 q (make-queue)
	     (list (blocking-queue-rear q)
		   (blocking-queue-rear q)))))

  (test* "blocking-queue-push!" '(bar foo)
	 (let1 q (make-blocking-queue)
	   (blocking-queue-push! q 'foo)
	   (blocking-queue-push! q 'bar)
	   (blocking-queue-dequeue-all! q)))

  (test* "blocking-queue-length" 0
	 (blocking-queue-length (make-blocking-queue)))

  (let ()
    (define (test-length add)
      (let1 q (make-blocking-queue)
	(add q 'foo)
	(blocking-queue-length q)))

    (test* "blocking-queue-length" 1
	   (test-length blocking-queue-enqueue!))
    (test* "blocking-queue-length" 1
	   (test-length blocking-queue-push!)))

  (let ()
    (define (test-length add remove)
      (let1 q (make-blocking-queue)
	(add q 'foo)
	(add q 'foo)
	(remove q)
	(blocking-queue-length q)))

    (test* "blocking-queue-length" 1
	   (test-length blocking-queue-enqueue! blocking-queue-dequeue!))
    (test* "blocking-queue-length" 1
	   (test-length blocking-queue-push! blocking-queue-pop!)))

  (test* "blocking-queue-enqueue! (timeout)" #f
         (blocking-queue-enqueue! (make-blocking-queue 0) 'foo 0.1))
  (test* "blocking-queue-push! (timeout)" #f
	 (blocking-queue-push! (make-blocking-queue 0) 'foo 0.1))
  (test* "blocking-queue-dequeue! (timeout)" #f
	 (blocking-queue-dequeue! (make-blocking-queue) 0.1))
  (test* "blocking-queue-pop! (timeout)" #f
	 (blocking-queue-pop! (make-blocking-queue) 0.1))
  (test* "blocking-queue-front (timeout)" #f
	 (blocking-queue-front (make-blocking-queue) 0.1))
  (test* "blocking-queue-rear (timeout)" #f
	 (blocking-queue-rear (make-blocking-queue) 0.1))

  (test* "blocking-queue-remaining-capacity" #f
	 (blocking-queue-remaining-capacity (make-blocking-queue)))
  (test* "blocking-queue-remaining-capacity" 100
	 (blocking-queue-remaining-capacity (make-blocking-queue 100)))
  (test* "blocking-queue-remaining-capacity" 99
	 (let1 q (make-blocking-queue 100)
	   (blocking-queue-enqueue! q 'foo)
	   (blocking-queue-remaining-capacity q)))

  (let ()
    (define (test-capacity add remove)
      (let1 q (make-blocking-queue 100)
	(add q 'foo)
	(add q 'foo)
	(remove q)
	(blocking-queue-remaining-capacity q)))

    (test* "blocking-queue-remaining-capacity" 99
	   (test-capacity blocking-queue-enqueue! blocking-queue-dequeue!))
    (test* "blocking-queue-remaining-capacity" 99
	   (test-capacity blocking-queue-push! blocking-queue-pop!)))

  (let ()
    (define (test-add add remove)
      (let1 q (make-blocking-queue 1)
	(blocking-queue-enqueue! q 'foo)
	(let1 producer
	    (make-thread (lambda () (blocking-queue-enqueue! q 'bar)))
	  (thread-start! producer)
	  (thread-yield!)
	  (blocking-queue-dequeue! q)
	  (thread-join! producer)
	  (blocking-queue-dequeue! q))))

    (test* "blocking-queue-enqueue! (blocking)" 'bar
	   (test-add blocking-queue-enqueue! blocking-queue-dequeue!))
    (test* "blocking-queue-push! (blocking)" 'bar
	   (test-add blocking-queue-push! blocking-queue-pop!)))

  (let ()
    (define (test-remove add remove)
      (let* ((q (make-blocking-queue))
	     (producer (make-thread
			(lambda ()
			  (thread-yield!)
			  (blocking-queue-enqueue! q 'foo)))))
	(thread-start! producer)
	(begin0
	 (blocking-queue-dequeue! q)
	 (thread-join! producer))))

    (test* "blocking-queue-dequeue! (blocking)" 'foo
	   (test-remove blocking-queue-enqueue! blocking-queue-dequeue!))
    (test* "blocking-queue-pop! (blocking)" 'foo
	   (test-remove blocking-queue-push! blocking-queue-pop!)))

  (test* "blocking-queue-dequeue-all! (blocking)" '(1 2)
	 (let1 q (make-blocking-queue 2)
	   (blocking-queue-enqueue! q 'a)
	   (blocking-queue-enqueue! q 'b)
	   (let ((thread1 (make-thread
			   (lambda () (blocking-queue-enqueue! q 1))))
		 (thread2 (make-thread
			   (lambda () (blocking-queue-enqueue! q 2)))))
	     (thread-start! thread1)
	     (thread-start! thread2)
	     (thread-yield!)
	     (blocking-queue-dequeue-all! q)
	     (thread-join! thread1)
	     (thread-join! thread2)
	     (sort (blocking-queue-dequeue-all! q)))))


  (let ()
    (define (test-peek peek)
      (let* ((q (make-blocking-queue))
	     (producer (make-thread
			(lambda ()
			  (thread-yield!)
			  (blocking-queue-enqueue! q 'foo)))))
	(thread-start! producer)
	(begin0
	 (peek q)
	 (thread-join! producer))))

    (test* "blocking-queue-front (blocking)" 'foo
	   (test-peek blocking-queue-front))
    (test* "blocking-queue-rear (blocking)" 'foo
	   (test-peek blocking-queue-rear)))
  )

;;-----------------------------------------------
(test-section "concurrent.future")

(use concurrent.future)
(unless (eq? (gauche-thread-type) 'none)
  (test* "future?" #t
	 (future? (make-future (cut #f))))
  (test* "future?" #f
	 (future? 'foo))

  (define thunk (lambda () (+ 1 2)))
  
  (test* "future-run!" 3
	 (let1 future (make-future thunk)
	   (future-run! future)
	   (future-get future)))

  (test* "completion-hook" 'ok
         (let1 var 'foo
           (define (hook future)
             (and (future? future)
                  (set! var 'ok)))
           (define future (make-future thunk :completion-hook hook))
           (future-run! future)
           (and (= 3 (future-get future))
                var)))

  (test* "completion-hook (error)" 'ok
         (let1 future (make-future thunk :completion-hook (lambda _ (raise 'ok)))
           (guard (e (else e))
             (future-run! future))))

  (test* "future-get (error)" 'ok
         (let1 future (make-future (lambda () (raise 'ok)))
	   (future-run! future)
           (guard (e (else e))
             (future-get future))))

  (test* "future-done?" #f
	 (future-done? (make-future thunk)))
  (test* "future-done?" #t
	 (let1 future (make-future thunk)
	   (future-run! future)
	   (future-done? future)))

  (test* "future-cancel!" 'ok
	 (let1 future (make-future thunk)
	   (future-cancel! future)
	   (guard (e (else
		      (and (is-a? e <future-cancelled-condition>)
			   'ok)))
	     (future-get future))))
  (test* "future-cancel!" 3
	 (let1 future (make-future thunk)
	   (future-run! future)
	   (future-cancel! future)
	   (future-get future)))
  (test* "future-cancel! and completion-hook" 'ok
	 (let1 var 'foo
           (define (hook future) (set! var 'ok))
           (define future (make-future thunk :completion-hook hook))
	   (future-cancel! future)
           var))

  (test* "future-get (blocking)" 3
	 (let* ((future (make-future thunk))
		(thread (make-thread
			 (lambda ()
			   (thread-yield!)
			   (future-run! future)))))
	   (thread-start! thread)
	   (begin0
	    (future-get future)
	    (thread-join! thread))))
  (test* "future-cancel! (blocking)" 'ok
	 (let* ((future (make-future thunk))
		(thread (make-thread
			 (lambda ()
			   (thread-yield!)
			   (future-cancel! future)))))
	   (thread-start! thread)
	   (guard (e (else
		      (thread-join! thread)
		      (and (is-a? e <future-cancelled-condition>)
			   'ok)))
	     (future-get future))))
  )

;;-----------------------------------------------
(test-section "concurrent.latch")
(use concurrent.latch)

(unless (eq? (gauche-thread-type) 'none)

  (test* "make-count-down-latch" 'ok
         (begin
           (make-count-down-latch 2)
           'ok))

  (test* "latch-dec!" "abc"
         (let ((out (open-output-string))
               (latch1 (make-count-down-latch 1))
               (latch2 (make-count-down-latch 2)))
           (define (task1)
             (latch-await latch1)
             (display "b" out)
             (latch-dec! latch2))
           (define (task2)
             (latch-dec! latch2)
             (display "a" out)
             (latch-dec! latch1))
           (thread-start! (make-thread task1))
           (thread-start! (make-thread task2))
           (latch-await latch2)
           (display "c" out)
           (get-output-string out)))
  )

;;-----------------------------------------------
(test-section "concurrent.barrier")
(use concurrent.barrier)

(unless (eq? (gauche-thread-type) 'none)
  (test* "make-cyclic-barrier" 'ok
         (begin (make-cyclic-barrier 3)
                'ok))

  (test* "barrier-await" "111222333444555"
         (let ((barrier (make-cyclic-barrier 3))
               (latch (make-count-down-latch 3))
               (out (open-output-string)))
           (define (task)
             (barrier-await barrier) (display "1" out)
             (barrier-await barrier) (display "2" out)
             (barrier-await barrier) (display "3" out)
             (barrier-await barrier) (display "4" out)
             (barrier-await barrier) (display "5" out)
             (latch-dec! latch))
           (dotimes (_ 3)
             (thread-start! (make-thread task)))
           (latch-await latch)
           (get-output-string out)))

  (test* "barrier-await" "baaa"
         (let* ((out (open-output-string))
                (barrier1 (make-cyclic-barrier 3 (lambda () (display "b" out))))
                (barrier2 (make-cyclic-barrier 3))
                (latch (make-count-down-latch 3)))
           (define (task)
             (barrier-await barrier1)
             (barrier-await barrier2)
             (display "a" out)
             (latch-dec! latch))
           (dotimes (_ 3)
             (thread-start! (make-thread task)))
           (latch-await latch)
           (get-output-string out)))
  )

;;-----------------------------------------------
(test-section "concurrent.executor")
(use concurrent.executor)
(use srfi-1)
(use srfi-42)
(test-module 'concurrent.executor)

(define executor #f)

(unless (eq? (gauche-thread-type) 'none)
  (test* "make <thread-pool-executor>" '(0 running)
         (begin
           (set! executor (make <thread-pool-executor> :core-pool-size 3))
           (list (ref executor 'current-pool-size)
                 (ref executor '%state))))

  (test* "make <thread-pool-executor>" *test-error*
         (make <thread-pool-executor> :core-pool-size -1))

  (test* "make <thread-pool-executor>" *test-error*
         (let1 executor (make <thread-pool-executor>)
           ;; raise an error because core-pool-size is not set
           (executor-submit executor (lambda () 'foo))))

  (test* "executor-submit" '(ok 1)
         (let* ((future (executor-submit executor (lambda () 'ok)))
                (val (and (future? future)
                          (future-get future))))
           (list val
                 (ref executor 'current-pool-size))))
  
  (test* "executor-submit" '(ok 3)
         (let1 barrier (make-cyclic-barrier 2)
           (define (task)
             (barrier-await barrier)
             'ok)
           (let1 futures (list-ec (: _ 10) (executor-submit executor task))
             (and (every (lambda (e) (eq? (future-get e) 'ok))
                         futures)
                  (list 'ok (ref executor 'current-pool-size))))))

  (test* "executor-submit (maximum-pool-size)" '(ok 5)
         (let ((executor (make <thread-pool-executor>
                           :core-pool-size 3
                           :maximum-pool-size 5))
               (barrier (make-cyclic-barrier 5)))
           (define (task)
             (barrier-await barrier)
             'ok)
           (let1 futures (list-ec (: _ 10) (executor-submit executor task))
             (and (every (lambda (e) (eq? (future-get e) 'ok))
                         futures)
                  (list 'ok (ref executor 'current-pool-size))))))

  (test* "executor-submit (rejected)" 'rejected
         (let ((executor (make <thread-pool-executor>
                           :core-pool-size 1
                           :work-queue (make-blocking-queue 1)
                           :rejected-execution-handler
                           (lambda (task executor)
                             (and (procedure? task)
                                  (is-a? executor <thread-pool-executor>)
                                  (raise 'rejected)))))
               (latch (make-count-down-latch 1))
               (barrier (make-cyclic-barrier 2)))
           (define (task)
             (latch-dec! latch)
             (barrier-await barrier)
             'ok)
           (executor-submit executor task)
           (latch-await latch)
           (executor-submit executor task)
           (guard (e (else (barrier-await barrier) e))
             (executor-submit executor task))))

  (test* "executor-shutdown!" 'ok
         (begin (executor-shutdown! executor)
                (executor-await-termination executor 5)
                'ok))

  ;; Completion service
  (test* "make <completion-service>" 'ok
         (let* ((executor (make <thread-pool-executor> :core-pool-size 3))
                (service (make <completion-service> :executor executor)))
           'ok))

  (test* "completion-service-submit" 'bar
         (let* ((executor (make <thread-pool-executor> :core-pool-size 3))
                (service (make <completion-service> :executor executor))
                (latch (make-count-down-latch 1))
                (future1 (completion-service-submit service (lambda () (latch-await latch) 'foo)))
                (future2 (completion-service-submit service (lambda () 'bar))))
           (unless (and (future? future1) (future? future2))
             (error "future expeced"))
           (let1 future (completion-service-get service)
             (begin0 (future-get future)
                     (latch-dec! latch)
                     (executor-shutdown! executor)
                     (executor-await-termination executor)))))
  )

(test-end)
