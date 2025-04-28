.PHONY: dist test clean all

ifeq ($(GO_CMD),)
GO_CMD:=go
endif

DIR_BIN := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))/bin
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

ifeq ($(GORELEASER_CMD),)
GORELEASER_CMD=$(TOOL_GORELEASER)
BUILD_DEP=$(TOOL_GORELEASER)
endif

TOOL_GORELEASER = $(DIR_BIN)/goreleaser
TOOL_STATICCHECK = $(DIR_BIN)/staticcheck
TOOLS = \
	$(TOOL_GORELEASER) \
	$(TOOL_STATICCHECK)

TOOLS_DEP = Makefile

.PHONY: tools
tools: $(TOOLS)
	@echo "$@ done." 1>&2

.PHONY: dist
dist: $(DISTS)
	@echo "$@ done." 1>&2

.PHONY: lint
lint: $(TOOL_STATICCHECK)
	$(TOOL_STATICCHECK) ./...

$(TOOL_STATICCHECK): export GOBIN=$(DIR_BIN)
$(TOOL_STATICCHECK): $(TOOLS_DEP)
	@echo "### `basename $@` install destination=$(GOBIN)" 1>&2
	CGO_ENABLED=0 $(GO_CMD) install honnef.co/go/tools/cmd/staticcheck@latest

$(TOOL_GORELEASER): export GOBIN=$(DIR_BIN)
$(TOOL_GORELEASER): $(TOOLS_DEP)
	@echo "### `basename $@` install destination=$(GOBIN)" 1>&2
	$(GO_CMD) install github.com/goreleaser/goreleaser/v2@v2.8.2

$(DIR_DIST)/athena-query: $(SRCS_OTHER) $(BUILD_DEP)
	$(GORELEASER_CMD) build --single-target --snapshot --clean -o $@
