GO=go
GOCMDDIRS=$(shell ./list_bindirs.sh)
BVER=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
ACTION=build
RACE=
FLAGS=-ldflags "-X main.buildTS=$(BVER)"

.PHONY: $(GOCMDDIRS)

install: ACTION=install

debug: RACE=-race

all: $(GOCMDDIRS)

install: $(GOCMDDIRS)

debug: $(GOCMDDIRS)

$(GOCMDDIRS):
	${GO} ${ACTION} ${RACE} ${FLAGS} $@
