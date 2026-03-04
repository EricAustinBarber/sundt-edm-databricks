set shell := ["bash", "-lc"]
set windows-shell := ["C:/Program Files/Git/bin/bash.exe", "-lc"]

python := if env_var_or_default("PYTHON", "") != "" { env_var("PYTHON") } else if os() == "windows" { "py -3.10" } else { "python3.10" }
venv_dir := ".venv"
venv_python := if os() == "windows" { venv_dir + "/Scripts/python.exe" } else { venv_dir + "/bin/python" }

default:
  @just --list

doctor:
  @{{python}} -c "import sys; print('python', sys.version)"
  @{{python}} -c "import sys; assert sys.version_info[:2] == (3, 10), 'Python 3.10 is required'; print('ok: python 3.10')"

venv:
  @{{python}} -m venv {{venv_dir}}
  @{{venv_python}} -m pip install --upgrade pip

clean-venv:
  @if [ -d "{{venv_dir}}" ]; then rm -rf "{{venv_dir}}"; fi

install: doctor venv
  @{{venv_python}} -m pip install -e .

run config="config/sources.yaml": install
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli run --config "$cfg"

smoke config="config/sources.yaml": install
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli smoke --config "$cfg"

smoke-ci: install
  @{{venv_python}} -m sundt_edm_quality.cli smoke --config config/sources.yaml --json-out artifacts/smoke.json

smoke-ci-out out: install
  @{{venv_python}} -m sundt_edm_quality.cli smoke --config config/sources.yaml --json-out {{out}}

discover-alation: install
  @{{venv_python}} -m sundt_edm_quality.cli discover-alation --config config/sources.yaml --json-out artifacts/alation-discovery.json

discover-bigeye: install
  @{{venv_python}} -m sundt_edm_quality.cli discover-bigeye --config config/sources.yaml --json-out artifacts/bigeye-discovery.json

scorecard-deploy: install
  @{{venv_python}} -m sundt_edm_quality.cli deploy-scorecard --config config/sources.yaml

scorecard-bootstrap config="config/sources.yaml": install
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli bootstrap-scorecard --config "$cfg"

scorecard-bootstrap-cluster cluster_id config="config/sources.yaml": install
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli bootstrap-scorecard --config "$cfg" --cluster-id {{cluster_id}}

scorecard-bootstrap-deploy config="config/sources.yaml": install
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli bootstrap-scorecard --config "$cfg"
  @cfg="{{config}}"; cfg="${cfg#config=}"; {{venv_python}} -m sundt_edm_quality.cli deploy-scorecard --config "$cfg"

compile: install
  @{{venv_python}} -m compileall src
