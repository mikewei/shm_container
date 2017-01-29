all: doc

CXXFLAGS = -I../src --std=c++11 
CLDOCOPT = $(addprefix --merge , $(wildcard *.md)) --type html --output .

doc:
	cldoc generate $(CXXFLAGS) -- $(CLDOCOPT) ../src/shmc/*

server:
	cldoc serve .

clean:
	rm -rf *.html *.json xml

.PHONY: all doc clean

