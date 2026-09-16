"""Rebuild the tiny Java batch fixture using javac 11+ and deterministic ZIP metadata."""
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from zipfile import ZipFile, ZipInfo, ZIP_STORED

root = Path(__file__).parent
with TemporaryDirectory(prefix="fabricqueryr-java-") as directory:
    subprocess.run(
        ["javac", "--release", "8", "-g:none", "-d", directory,
         str(root / "FabricQueryRBatchProbe.java")],
        check=True,
    )
    with ZipFile(root / "livy-batch-java.jar", "w") as archive:
        info = ZipInfo("FabricQueryRBatchProbe.class", date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_STORED
        archive.writestr(info, (Path(directory) / info.filename).read_bytes())
