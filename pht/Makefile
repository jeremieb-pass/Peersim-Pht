.PHONY: all clean doc compile

#PEERSIM_JARS="../peersim-1.0.5"
#LIB_JARS=`find -L lib/ -name "*.jar" | tr [:space:] :`
#LIB_JARS=../peersim-1.0.5/jep-2.3.0.jar:../peersim-1.0.5/peersim-1.0.5.jar:../peersim-1.0.5/peersim-doclet.jar:../pastry/classes
LIB_JARS=lib/djep-1.0.0.jar:lib/jep-2.3.0.jar:lib/peersim-1.0.5.jar:lib/commons-codec-1.10.jar:../pastry/classes:lib/junit-4.7.jar

compile:
	mkdir -p classes
	javac -source 1.8 -sourcepath src -classpath $(LIB_JARS):$(PEERSIM_JARS) -d classes `find -L -name "*.java"`

run:
	@(echo $(PEERSIM_JARS) ++ $(LIB_JARS))
	java -cp $(LIB_JARS):classes peersim.Simulator examplePHText.cfg

all: compile run


doc:
	mkdir -p doc
	javadoc -sourcepath src -classpath $(LIB_JARS) -d doc -subpackages peersim.pht
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc peersim.pht
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc/dht peersim.pht.dht
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc/examples peersim.pht.examples.mspastry
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc/exceptions peersim.pht.exceptions
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc/messages peersim.pht.messages
	#javadoc -sourcepath src -classpath $(LIB_JARS) -d doc/state peersim.pht.state


clean: 
	rm -fr classes../peersim-1.0.5/djep-1.0.0.jar



testNode:
	javac -sourcepath src -d classes src/peersim/pht/PhtNode.java src/peersim/pht/tests/testPhtData.java src/peersim/pht/tests/testPhtNode.java

runNode:
	java -cp classes peersim.pht.testsTmp.testPhtNode


testProtocol: testNode
	javac -sourcepath src -d classes src/peersim/pht/PhtProtocol.java src/peersim/pht/tests/testPhtProtocolDHTI.java src/peersim/pht/tests/testPhtProtocol.java

runProtocol:
	java -cp classes peersim.pht.testsTmp.testPhtProtocol
