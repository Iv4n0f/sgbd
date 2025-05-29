TARGET = main

SRCS = $(wildcard *.cpp)

OBJS = $(SRCS:.cpp=.o)

CC = g++
CFLAGS = -Wall -O2

all: $(TARGET)
	@./$(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) *.o
