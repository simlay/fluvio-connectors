TARGET?=x86_64-unknown-linux-musl
RUSTV?=stable
BUILD_PROFILE=$(if $(RELEASE),release,debug)
RELEASE_FLAG=$(if $(RELEASE),--release,)
TARGET_FLAG=$(if $(TARGET),--target $(TARGET),)

# These defaults are set for development purposes only. CI will override
CONNECTOR_NAME?=test-connector
IMAGE_NAME?=infinyon/fluvio-connect-$(CONNECTOR_NAME)
CONNECTOR_BIN=$(if $(TARGET),./target/$(TARGET)/$(BUILD_PROFILE)/$(CONNECTOR_NAME),./target/$(BUILD_PROFILE)/$(CONNECTOR_NAME))

smoke-test:
	cargo run --bin fluvio-connector start ./test-connector/config.yaml

ifndef CONNECTOR_NAME
build:
	cargo build $(TARGET_FLAG) $(RELEASE_FLAG)
else
build:
	cargo build $(TARGET_FLAG) $(RELEASE_FLAG) --bin $(CONNECTOR_NAME)
endif

ifeq (${CI},true)
# In CI, we expect all artifacts to already be built and loaded for the script
copy-binaries:
else
# When not in CI (i.e. development), build and copy the binaries alongside the Dockerfile
copy-binaries: build
	cp $(CONNECTOR_BIN) container-build
endif

official-container: copy-binaries
	cd container-build && \
		docker build -t $(IMAGE_NAME) --build-arg CONNECTOR_NAME=$(CONNECTOR_NAME) .

CONNECTOR_LIST = syslog test-connector
official-containers:
	$(foreach var,$(CONNECTOR_LIST),CONNECTOR_NAME=$(var) make official-container;)

clean:
	cargo clean
	rm -f container-build/test-connector
	rm -f container-build/syslog


FLUVIO_CONNECTOR=cargo run --bin fluvio-connector
smoke-test:
	$(FLUVIO_CONNECTOR) create --config ./test-connector/config.yaml
	$(FLUVIO_CONNECTOR) list
