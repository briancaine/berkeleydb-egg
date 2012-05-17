all : berkeleydb.so

berkeleydb.so : berkeleydb-src.scm berkeleydb.scm
	csc -shared -J berkeleydb.scm -o berkeleydb.so -ldb

clean :
	rm -f *.import.scm *.so
