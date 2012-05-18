(import chicken scheme)
(import foreign)
(use lolevel ports)

(foreign-declare
  "#include <db.h>
   #include <errno.h>")

(define +default-allocation+ 30)

(define DB_CREATE (foreign-value "DB_CREATE" int))
(define DB_EXCL (foreign-value "DB_EXCL" int))
(define DB_RDONLY (foreign-value "DB_RDONLY" int))
(define DB_TRUNCATE (foreign-value "DB_TRUNCATE" int))

(define DB_BTREE (foreign-value "DB_BTREE" int))
(define DB_HEAP (foreign-value "DB_HEAP" int))
(define DB_QUEUE (foreign-value "DB_QUEUE" int))
(define DB_RECNO (foreign-value "DB_RECNO" int))
(define DB_UNKNOWN (foreign-value "DB_UNKNOWN" int))

(define DB_READ_UNCOMMITTED (foreign-value "DB_READ_UNCOMMITTED" int))
(define DB_THREAD (foreign-value "DB_THREAD" int))
(define DB_MULTIVERSION (foreign-value "DB_MULTIVERSION" int))
(define DB_NOMMAP (foreign-value "DB_NOMMAP" int))
(define DB_AUTO_COMMIT (foreign-value "DB_AUTO_COMMIT" int))

(define EINVAL (foreign-value "EINVAL" int))

(define DB_BUFFER_SMALL (foreign-value "DB_BUFFER_SMALL" int))

(define +db-pointer-size+
  (max (foreign-value "sizeof(DB**)" int)
       (foreign-value "sizeof(DB*)" int)))

(define DB*-pointer-ref
  (foreign-lambda* c-pointer ((c-pointer x))
    "C_return(x);"))

(define (data->string x len)
  (let ((res (make-string len)))
    (move-memory! x res len)
    res))

(define-record berkeley-db ptr path)
(define-record-printer (berkeley-db db port)
  (with-output-to-port port
     (lambda ()
        (printf "#<berkeley-db ~a on ~s>"
                (or (berkeley-db-ptr db) "(closed)")
                (berkeley-db-path db)))))

(define db-create
  (let ((int-db-create
           (foreign-lambda int db_create c-pointer c-pointer unsigned-int)))
    (lambda (#!key dbenv (flags 0))
      (let* ((res-ptr (allocate (* +db-pointer-size+ 2)))
             (result (int-db-create res-ptr dbenv flags))
             (final-ptr (let ((x (DB*-pointer-ref res-ptr)))
                          (free res-ptr)
                          x)))
        (cond
          ((zero? result) (make-berkeley-db final-ptr #f))
          ((equal? EINVAL result) (error "Invalid flag or parameter"))
          (else (error "Unknown error")))))))

(define-syntax db-func
  (er-macro-transformer
   (lambda (x r c)
     (let* ((type (cadr x))
            (name (caddr x))
            (var-types (cdddr x))
            (vars (map (lambda (type offset)
                          (list type
                                (string->symbol
                                  (string (integer->char (+ 65 offset))))))
                       var-types
                       (iota (length var-types))))
           )
      `(,(r 'foreign-lambda*) ,type ,vars
          ,(format "C_return(((DB*)A)->~a(~a));"
                   name
                   (string-join (map (compose ->string cadr) vars) ", ")))))))

(define db-open!
  (let ((int-db-open (db-func int open c-pointer c-pointer c-string c-string
                              unsigned-int unsigned-int int)))
    (lambda (db filename #!key txnid database (type DB_UNKNOWN) (mode 0)
                               (flags 0))
      (let ((res (int-db-open (berkeley-db-ptr db)
                    txnid filename database type flags mode)))
        (cond
          ((zero? res) #t)
          (else (error "Some sort of error")))))))

(define db-put!
  (let ((int-db-put
           (foreign-lambda* int
             ((c-pointer db) (c-pointer txnid)
              (c-string key) (int keylen)
              (c-string data) (int datalen)
              (int flags))
             "DBT a, b;
              a.data = key;
              a.size = keylen;
              b.data = data;
              b.size = datalen;
              C_return(((DB*)db)->put(db, txnid, &a, &b, flags));")))
    (lambda (db key value #!key txnid (flags 0))
      (let ((res (int-db-put (berkeley-db-ptr db) txnid
                             key (string-length key)
                             value (string-length value)
                             flags)))
        (cond
          ((zero? res) #t)
          (else (error "Some sort of error")))))))

(define db-get
  (let ((int-db-get
           (foreign-lambda* int
             ((c-pointer db) (c-pointer txnid)
              (c-string key) (int keylen)
              (c-string data) (int datalen)
              (int flags))
             "DBT a, b;
              a.data = key;
              a.size = keylen;
              b.data = data;
              b.size = datalen;
              C_return(((DB*)db)->get(db, txnid, &a, &b, flags));")))
    (lambda (db key #!key txnid (flags 0))
      (define (try count)
        (let* ((data (allocate count))
               (res (int-db-get (berkeley-db-ptr db) txnid
                                key (string-length key)
                                data count flags))
               (res-str (and (zero? res) (data->string data count))))
          (free data)
          (cond
            ((zero? res) res-str)
            ((equal? res DB_BUFFER_SMALL) (try (* count 2)))
            (else (error "Unknown error")))))
      (try +default-allocation+))))


