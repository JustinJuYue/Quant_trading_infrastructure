.PHONY: bootstrap run run-manual clean

bootstrap:
	chmod +x scripts/bootstrap.sh scripts/run_local.sh
	./scripts/bootstrap.sh

run:
	chmod +x scripts/run_local.sh
	./scripts/run_local.sh

run-manual:
	@echo "Terminal A:"
	@echo "  cd julia_app && julia --project=. server.jl"
	@echo "Terminal B:"
	@echo "  cd python_app && source .venv/bin/activate && python client.py"

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} \;
	find . -type f -name "*.pyc" -delete