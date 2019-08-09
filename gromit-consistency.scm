;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit-consistency - Node IDs and counters for timestamps and seqnos.
;;;
;;; gromit manages a set of typed objects that can be persisted to an SQLite
;;; database. The objects can be linked together with labelled edges.
;;;
;;; gromit objects are made up of attributes and properties. Attributes are
;;; simple, single valued data fields. Properties are lists of sets of fields.
;;; The user can select from the built-in data types for fields or define their
;;; own. Edges are much simpler: they specify a start point, an end point, have
;;; a label and are directional.
;;;
;;;
;;;  Copyright (C) 2019, Andy Bennett, Register Dynamics Limited.
;;;  All rights reserved.
;;;
;;;  Redistribution and use in source and binary forms, with or without
;;;  modification, are permitted provided that the following conditions are met:
;;;
;;;  Redistributions of source code must retain the above copyright notice, this
;;;  list of conditions and the following disclaimer.
;;;  Redistributions in binary form must reproduce the above copyright notice,
;;;  this list of conditions and the following disclaimer in the documentation
;;;  and/or other materials provided with the distribution.
;;;  Neither the name of the author nor the names of its contributors may be
;;;  used to endorse or promote products derived from this software without
;;;  specific prior written permission.
;;;
;;;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;;;  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;;;  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;;  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
;;;  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
;;;  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
;;;  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;;  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
;;;  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
;;;  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
;;;  POSSIBILITY OF SUCH DAMAGE.
;;;
;;; Andy Bennett <andyjpb@register-dynamics.co.uk>, 2019/07/18
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module gromit-consistency
	(;; Node ID
	 gromit-node-id
	 set-gromit-node-id!

	 ; Counters
	 initialise-persistent-counter
	 load-persistent-counter

	 ; Sequence Numbers
	 gromit-next-seqno
	 set-gromit-seqno-proc!

	 ; Timestamps
	 gromit-next-timestamp
	 set-gromit-timestamp-proc!
	 )

(import chicken scheme)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures extras posix srfi-18)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
;(use )



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Fundamental Data Consistency

; Node ID

(define gromit-node-id 'gromit-node-id-not-set)


(define (set-gromit-node-id! node-id)
  (assert (and
	    (integer? node-id)
	    (exact?   node-id)
	    (> node-id 0))
	  (conc "set-gromit-node-id!: Expecting an integer node-id greater than 0. We got " node-id "!"))
  (set! gromit-node-id node-id))


; Counters and state

(define (load-counter state-file)
  (if (regular-file? state-file)
    (car (read-file state-file))
    (abort (conc state-file " not found!"))))


; Assumes we hold the seqno mutex
(define (save-counter state-file seqno)
  (with-output-to-file
    state-file
    (lambda ()
      (write seqno))))


(define (initialise-persistent-counter state-file)
  (assert (not (or
		 (regular-file?     state-file)
		 (socket?           state-file)
		 (block-device?     state-file)
		 (character-device? state-file)
		 (directory?        state-file)))
	  (conc "initialise-persistent-counter: File " state-file " already exists!"))
  (save-counter state-file 0))


(define (load-persistent-counter state-file)
  (let ((counter (load-counter state-file))
	(m       (make-mutex "persistent-counter")))
    (lambda ()
      (if (mutex-lock! m)
	(let ((current counter))
	  (set! counter (+ 1 counter))
	  (save-counter state-file counter)
	  (mutex-unlock! m)
	  current)
	(abort "persistent-counter mutex error")))))


; Sequence Numbers

(define gromit-next-seqno
  (lambda ()
    (abort "gromit-next-seqno: not configured. Use set-gromit-seqno-proc!")))


(define (set-gromit-seqno-proc! proc)
  (set! gromit-next-seqno proc))


; Timestamps

(define gromit-next-timestamp
  (lambda ()
    (abort "gromit-next-timestamp: not configured. Use set-gromit-timestamp-proc!")))


(define (set-gromit-timestamp-proc! proc)
  (set! gromit-next-timestamp proc))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

