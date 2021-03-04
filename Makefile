.PHONY: clean help

VERSION      := 0.0.1
PROJECTNAME  := kafka-admin-tool
SOURCES      := $(wildcard *.go)

# Packages to build before go get runs
PACKAGE_DIRS = adminutils actions


# Use linker flags to provide version/build settings
LDFLAGS=-ldflags "-X=main.Version=$(VERSION)"

# Make is verbose in Linux. Make it silent.
MAKEFLAGS += --silent

all: compile

## install: Install missing dependencies. Runs `go get` internally. e.g; make install get=github.com/foo/bar
install: go-get

## compile: Compile the binary.
compile: go-format go-compile

## clean: Clean build files. Runs `go clean` internally.
clean: clear go-clean

## format: Format all source files
format: go-format

clear:
	clear


go-compile: go-get go-build


go-get:
	@echo "--> Retrieving all dependencies"
	go get

go-build:
	@echo "--> Building linux binary"
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/$(PROJECTNAME)-linux-amd64 $(SOURCES)
	@echo "--> Building macos binary"
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/$(PROJECTNAME)-darwin-amd64 $(SOURCES)

go-clean:
	@echo "--> Cleaning build cache"
	rm -rf bin/*
	go clean

go-format:
	@echo "--> Formatting source files $(PACKAGES)"
	go fmt $(SOURCES)

help: Makefile
	@echo
	@echo " Choose a command to run in "$(PROJECTNAME)":"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo
