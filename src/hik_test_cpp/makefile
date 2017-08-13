CXX=g++ -std=c++11
CFLAGS+=-I./
LDFLAGS:=-L./
LIBS:=-lpthread -laws-cpp-sdk-core -laws-cpp-sdk-s3

SOURCES:=$(wildcard *.c) $(wildcard *.cpp)
OBJS:=$(patsubst %.c,%.o,$(patsubst %.cpp,%.o,$(SOURCES)))
TARGET=hik_tt

all:$(OBJS)
	@echo "source files:"$(SOURCES)
	@echo "object files:"$(OBJS)
	$(CXX) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS) $(LIBS)

include $(SOURCE:.c=.d)
%.d:%.c
	$(CXX) -MM $(CFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ ,g' < $@.$$$$ > $@; \
	rm $@.$$$$

clean:
	-rm -f $(OBJS)
