# Makefile
JAVAC=javac
JAVA=java
JAR=jar
CLASSES=classes
EXT_LIB=./dependencies
SRC=src/main/java
JARFILE=storm-learning.jar
LOG=storm-learning.log

target:
	@mkdir -p $(CLASSES)
	@find ${SRC}/ -name "*.java" > ${CLASSES}/sources.list
	@${JAVAC} -Djava.ext.dirs=${EXT_LIB} -nowarn -sourcepath ${SRC}/ -d ${CLASSES} @${CLASSES}/sources.list
	@rm -rf ${CLASSES}/sources.list
	@echo "compile successfully!"

jar:
	@${JAR} cvf ${JARFILE} -C ${CLASSES} .
	@echo "generate JAR file successfully!"

clean:
	@rm -rf ${CLASSES}
	@rm -rf ${JARFILE}
	@rm -rf *@*

test:
	@echo "start test..."
	@mvn test
