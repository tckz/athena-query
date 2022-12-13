.PHONY: dist test clean all

ifeq ($(GO_CMD),)
GO_CMD:=go
endif

VERSION := $(shell git describe --always)
GO_BUILD := $(GO_CMD) build -ldflags "-X main.version=$(VERSION)"

DIR_DIST = dist

DISTS = \
	$(DIR_DIST)/athena-query

TARGETS = $(DISTS)

SRCS_OTHER := $(shell find . \
	-type d -name cmd -prune -o \
	-type f -name "*.go" -print) go.mod

all: $(TARGETS)
	@echo "$@ done." 1>&2

clean:
	/bin/rm -f $(TARGETS)
	@echo "$@ done." 1>&2

$(DIR_DIST)/athena-query: cmd/athena-query/* $(SRCS_OTHER)
	$(GO_BUILD) -o $@ ./cmd/athena-query/
