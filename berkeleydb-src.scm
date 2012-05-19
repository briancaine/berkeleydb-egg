(import chicken scheme)
(import foreign)
(use lolevel ports srfi-4)

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
(define DB_HASH (foreign-value "DB_HASH" int))
(define DB_QUEUE (foreign-value "DB_QUEUE" int))
(define DB_RECNO (foreign-value "DB_RECNO" int))
(define DB_UNKNOWN (foreign-value "DB_UNKNOWN" int))

(define DB_READ_UNCOMMITTED (foreign-value "DB_READ_UNCOMMITTED" int))
(define DB_THREAD (foreign-value "DB_THREAD" int))
(define DB_MULTIVERSION (foreign-value "DB_MULTIVERSION" int))
(define DB_NOMMAP (foreign-value "DB_NOMMAP" int))
(define DB_AUTO_COMMIT (foreign-value "DB_AUTO_COMMIT" int))

(define EINVAL (foreign-value "EINVAL" int))
(define ENOENT (foreign-value "ENOENT" int))

(define DB_BUFFER_SMALL (foreign-value "DB_BUFFER_SMALL" int))

(define DB_NOTFOUND (foreign-value "DB_NOTFOUND" int))

(define DB_NEXT (foreign-value "DB_NEXT" int))

(define DB_CREATE (foreign-value "DB_CREATE" int))

(define +db-pointer-size+
  (max (foreign-value "sizeof(DB**)" int)
       (foreign-value "sizeof(DB*)" int)
       (foreign-value "sizeof(DBT*)" int)))
(define +dbt-size+ (foreign-value "sizeof(DBT)" int))

(define dbt->string
  (let ((DBT*-data (foreign-lambda* c-pointer ((c-pointer x))
                                    "C_return(((DBT*)x)->data);"))
        (DBT*-size (foreign-lambda* unsigned-int ((c-pointer x))
                                    "C_return(((DBT*)x)->size);")))
    (lambda (x)
      (let* ((size (DBT*-size x))
             (res (make-string size)))
        (move-memory! (DBT*-data x) res size)
        res))))

(define DB*-pointer-ref
  (foreign-lambda* c-pointer ((c-pointer x))
    "C_return(x);"))

(define DB**-pointer-ref
  (foreign-lambda* c-pointer ((c-pointer x))
    "C_return(*(DB**)x);"))

(define DBC*-pointer-ref
  (foreign-lambda* c-pointer ((c-pointer x))
    "DBC** ptr = x;
     C_return(*ptr);"))

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
             (final-ptr (let ((x (DB**-pointer-ref res-ptr)))
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
  (let (
        (int-db-open (foreign-lambda*
                      int ((c-pointer db) (c-pointer txnid) (c-string filename)
                           (c-string database) (unsigned-int type)
                           (unsigned-int flags) (int mode))
                      "C_return(((DB*)db)->open(
                         db, txnid, filename, database, type, flags, mode));"))
;        (int-db-open (db-func int open c-pointer c-pointer c-string c-string
;                              unsigned-int unsigned-int int))
        )
    (lambda (db filename #!key txnid database (type DB_UNKNOWN) (mode 0)
                               (flags 0))
      (let ((res (int-db-open (berkeley-db-ptr db)
                    txnid filename database type flags mode)))
        (cond
          ((zero? res) #t)
          ((equal? res ENOENT)
           (error "File or directory doesn't exist" filename))
          (else (error "Some sort of error")))))))

(define db-put!
  (let ((int-db-put
           (foreign-lambda* int
             ((c-pointer db) (c-pointer txnid)
              (c-string key) (int keylen)
              (c-string data) (int datalen)
              (int flags))
             "DBT a, b;
              memset(&a, 0, sizeof(DBT));
              memset(&b, 0, sizeof(DBT));
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
              (c-pointer data) (int datalen) (u32vector final_size)
              (int flags))
             "DBT a, b;
              memset(&a, 0, sizeof(DBT));
              memset(&b, 0, sizeof(DBT));
              a.data = key;
              a.size = keylen;
              b.data = data;
              b.ulen = datalen;
              b.flags = DB_DBT_USERMEM;
              int res = ((DB*)db)->get(db, txnid, &a, &b, flags);
              *(uint*)final_size = b.size;
              C_return(res);")))
    (lambda (db key #!key txnid (flags 0))
      (define (try count)
        (let* ((data (allocate count))
               (res-size (make-u32vector 1))
               (res (int-db-get (berkeley-db-ptr db) txnid
                                key (string-length key)
                                data count res-size
                                flags))
               (res-str (and (zero? res)
                             (data->string data (u32vector-ref res-size 0)))))
          (free data)
          (cond
            ((zero? res) res-str)
            ((equal? res DB_BUFFER_SMALL) (try (* count 2)))
            ((equal? res DB_NOTFOUND) (error "Key missing" key))
            (else (error "Unknown error")))))
      (try +default-allocation+))))

(define db-delete!
  (let ((int-db-del
           (foreign-lambda* int
             ((c-pointer db) (c-pointer txnid)
              (c-string key) (int keylen)
              (int flags))
             "DBT a;
              memset(&a, 0, sizeof(DBT));
              a.data = key;
              a.size = keylen;
              C_return(((DB*)db)->del(db, txnid, &a, flags));")))
    (lambda (db key #!key txnid (flags 0))
      (let ((res (int-db-del (berkeley-db-ptr db) txnid
                             key (string-length key)
                             flags)))
        (cond
          ((zero? res) #t)
          (else (error "Some sort of error")))))))

(define db-close!
  (let ((int-db-close (foreign-lambda*
                       int ((c-pointer db) (unsigned-int flags))
                       "C_return(((DB*)db)->close(db, flags));")))
    (lambda (db #!key (flags 0))
      (let ((res (int-db-close (berkeley-db-ptr db) flags)))
        (cond
         ((zero? res)
          (berkeley-db-ptr-set! db #f)
          #t)
         (else (error "Some sort of error")))))))

(define db-sync
  (let ((int-db-sync (foreign-lambda*
                      int ((c-pointer db) (unsigned-int flags))
                      "C_return(((DB*)db)->sync(db, flags));")))
    (lambda (db #!key (flags 0))
      (let ((res (int-db-sync (berkeley-db-ptr db) flags)))
        (cond
         ((zero? res) #t)
         (else (error "Some sort of error")))))))

;; cursors
(define-record berkeley-db-cursor ptr db)
(define-record-printer (berkeley-db-cursor cursor port)
  (with-output-to-port port
     (lambda ()
        (printf "#<berkeley-db-cursor ~a on ~s>"
                (berkeley-db-cursor-ptr cursor)
                (berkeley-db-cursor-db cursor)))))

(define db-cursor
  (let ((int-db-cursor
         (foreign-lambda*
          int ((c-pointer db) (c-pointer txnid)
               (c-pointer cursorp) (unsigned-int flags))
          "C_return(((DB*)db)->cursor(db, txnid, cursorp, flags));")))
    (lambda (db #!key txnid (flags 0))
      (let* ((ptr (allocate +db-pointer-size+))
             (res (int-db-cursor (berkeley-db-ptr db) txnid ptr flags))
             (cursor (DBC*-pointer-ref ptr)))
        (free ptr)
        (cond
         ((zero? res) (make-berkeley-db-cursor cursor db))
         (else (error "Some sort of error")))))))

(define db-cursor-get/next
  (let ((int-db-cursor-get/next
         (foreign-lambda*
          int ((c-pointer cursor)
               (c-pointer key) (c-pointer data)
               (unsigned-int flags))
          "memset(key, 0, sizeof(DBT));
           memset(data, 0, sizeof(DBT));
           C_return(((DBC*)cursor)->get(cursor, key, data, flags));")))
    (lambda (cursor #!key (flags DB_NEXT))
      (let* ((key (allocate +dbt-size+))
             (data (allocate +dbt-size+))
             (res (int-db-cursor-get/next
                   (berkeley-db-cursor-ptr cursor)
                   key data flags))
             (key-res (and (zero? res) (dbt->string key)))
             (data-res (and (zero? res) (dbt->string data))))
        (free key)
        (free data)
        (cond
         ((zero? res) (cons key-res data-res))
         ((equal? res DB_NOTFOUND) '())
         (else (error "Some sort of error")))))))

(define db-cursor-close!
  (let ((int-db-cursor-close
         (foreign-lambda*
          int ((c-pointer cursor))
          "C_return(((DBC*)cursor)->get(cursor));")))
    (lambda (cursor)
      (let* ((res (int-db-cursor-get/next
                   (berkeley-db-cursor-ptr cursor)
                   key data flags)))
        (cond
         ((zero? res) #t)
         (else (error "Some sort of error")))))))
