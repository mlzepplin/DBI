
CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Statistics.o y.tab.o lex.yy.o main.o HeapFile.o SortedFile.o QueryPlanner.o Function.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Statistics.o y.tab.o lex.yy.o main.o -lpthread HeapFile.o SortedFile.o QueryPlanner.o Function.o

	
main.o : main.cc
	$(CC) -g -c main.cc

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o Statistics.o y.tab.o lex.yy.o test.o HeapFile.o SortedFile.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o Statistics.o y.tab.o lex.yy.o test.o -lpthread HeapFile.o SortedFile.o
	
test.o: test.cc
	$(CC) -g -c test.cc

a3test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o HeapFile.o SortedFile.o
	$(CC) -o a3test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lpthread HeapFile.o SortedFile.o

a3test.o: a3test.cc
	$(CC) -g -c a3test.cc

a22.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o HeapFile.o SortedFile.o
	$(CC) -o a22.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lpthread HeapFile.o SortedFile.o

a22.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o HeapFile.o SortedFile.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lpthread HeapFile.o SortedFile.o

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o HeapFile.o SortedFile.o BigQ.o
	$(CC) -o a1-test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o HeapFile.o SortedFile.o BigQ.o

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc 
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

QueryPlanner.o: QueryPlanner.h Schema.h ParseTree.h Statistics.h Comparison.h
	$(CC) -g -c QueryPlanner.cc  

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


# added HeapFile support
HeapFile.o: HeapFile.h DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Defs.h
	$(CC) -g -c HeapFile.cc

SortedFile.o: SortedFile.h DBFile.h Record.h Schema.h File.h Comparison.h ComparisonEngine.h Defs.h BigQ.h HeapFile.h
	$(CC) -g -c SortedFile.cc


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
