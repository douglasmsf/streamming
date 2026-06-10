import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Permite importar os modulos do gerador e do producer nos testes
sys.path.insert(0, str(ROOT / "generator"))
sys.path.insert(0, str(ROOT / "producer"))
