BUILD_DIR := build
BINARY    := $(BUILD_DIR)/cpp/characterization
RESULTS   := cpp/results
SCRIPTS   := cpp/scripts
NPROC     := $(shell sysctl -n hw.ncpu 2>/dev/null || nproc)

.PHONY: build run plot cms-point-query clean

# Build the C++ binary
build:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake .. && make -j$(NPROC)

# Run CMS point query profile and generate plot
cms-point-query: build
	$(BINARY) cms-point-query > $(RESULTS)/cms_point_query.tsv
	uv run $(SCRIPTS)/plot_cms_point_query.py $(RESULTS)/cms_point_query.tsv cms_point_query_error.svg

# Just rebuild and run (no plot)
run: build
	$(BINARY) cms-point-query > $(RESULTS)/cms_point_query.tsv

# Just regenerate the plot from existing TSV
plot:
	uv run $(SCRIPTS)/plot_cms_point_query.py $(RESULTS)/cms_point_query.tsv cms_point_query_error.svg

clean:
	rm -rf $(BUILD_DIR)
