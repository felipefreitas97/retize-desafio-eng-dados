import subprocess
import sys
from pathlib import Path
import logging


def setup_logger():
    logger = logging.getLogger('orquestrador')
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


logger = setup_logger()


def run_command(command, cwd=None, description=None):
    description = description or 'comando'
    logger.info(f'Iniciando {description}: {command}')

    try:
        subprocess.run(command, cwd=cwd, check=True)
        logger.info(f'{description.capitalize()} concluído com sucesso')
    except subprocess.CalledProcessError as exc:
        logger.error(f'Erro ao executar {description}: {exc}')
        raise


def run_ingestion():
    script_path = Path(__file__).resolve().parent / 'src' / 'ingestion.py'
    if not script_path.exists():
        raise FileNotFoundError(f'Não foi possível encontrar o script de ingestão em {script_path}')

    run_command([sys.executable, str(script_path)], description='ingestão')


def run_dbt():
    project_dir = Path(__file__).resolve().parent / 'dbt'
    if not project_dir.exists():
        raise FileNotFoundError(f'Não foi possível encontrar o diretório dbt em {project_dir}')

    venv_dir = Path(__file__).resolve().parent / '.venv'
    dbt_exe = venv_dir / 'Scripts' / 'dbt.exe' if sys.platform == 'win32' else venv_dir / 'bin' / 'dbt'
    
    run_command([str(dbt_exe), 'run'], cwd=project_dir, description='transformação dbt')


def run_streamlit():
    app_path = Path(__file__).resolve().parent / 'streamlit_app.py'
    if not app_path.exists():
        raise FileNotFoundError(f'Não foi possível encontrar o app Streamlit em {app_path}')

    run_command([sys.executable, '-m', 'streamlit', 'run', str(app_path)], description='app Streamlit')


def run():
    logger.info('Orquestração iniciada')
    run_ingestion()
    run_dbt()
    run_streamlit()
    logger.info('Orquestração finalizada')


if __name__ == '__main__':
    run()
