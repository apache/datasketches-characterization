BUILD_DIR := build
BINARY    := $(BUILD_DIR)/cpp/characterization
RESULTS   := cpp/results
SCRIPTS   := cpp/scripts
NPROC     := $(shell sysctl -n hw.ncpu 2>/dev/null || nproc)

.PHONY: build cms-point-query cms-point-query-plot clean

# Build the C++ binary
build:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake .. && make -j$(NPROC)

# Run CMS point query profile (all 3 skew regimes) and generate plots
cms-point-query: build
	$(BINARY) cms-point-query
	uv run $(SCRIPTS)/plot_cms_point_query.py \
		$(RESULTS)/cms_point_query_high_skew.tsv \
		$(RESULTS)/cms_point_query_medium_skew.tsv \
		$(RESULTS)/cms_point_query_low_skew.tsv
	@echo "Generated: cms_point_query_error.svg cms_rel_error_vs_freq_rank.svg"

# Just regenerate plots from existing TSV files
cms-point-query-plot:
	uv run $(SCRIPTS)/plot_cms_point_query.py \
		$(RESULTS)/cms_point_query_high_skew.tsv \
		$(RESULTS)/cms_point_query_medium_skew.tsv \
		$(RESULTS)/cms_point_query_low_skew.tsv
	@echo "Generated: cms_point_query_error.svg cms_rel_error_vs_freq_rank.svg"

clean:
	rm -rf $(BUILD_DIR)
