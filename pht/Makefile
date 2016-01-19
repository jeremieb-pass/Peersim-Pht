.PHONY: all clean doc compile

#PEERSIM_JARS="../peersim-1.0.5"
#LIB_JARS=`find -L lib/ -name "*.jar" | tr [:space:] :`
#LIB_JARS=../peersim-1.0.5/jep-2.3.0.jar:../peersim-1.0.5/peersim-1.0.5.jar:../peersim-1.0.5/peersim-doclet.jar:../pastry/classes
LIB_JARS=lib/djep-1.0.0.jar:lib/jep-2.3.0.jar:lib/peersim-1.0.5.jar:lib/commons-codec-1.10.jar:lib/mspastry.jar

compile:
	mkdir -p classes
	javac -source 1.8 -sourcepath src -classpath $(LIB_JARS):$(PEERSIM_JARS) -d classes `find -L -name "*.java"`

run:
	@(echo $(PEERSIM_JARS) ++ $(LIB_JARS))
	java -ea -cp $(LIB_JARS):classes peersim.Simulator examplePHT.cfg

all: compile run


doc:
	mkdir -p doc
	javadoc -sourcepath src -classpath $(LIB_JARS) -d doc peersim.pht


clean: 
	rm -fr classes../peersim-1.0.5/djep-1.0.0.jar

