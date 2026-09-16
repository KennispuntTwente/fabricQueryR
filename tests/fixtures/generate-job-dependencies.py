"""Generate deterministic, dependency-free archives for live job probes."""
from pathlib import Path
from zipfile import ZipFile, ZipInfo, ZIP_STORED

root = Path(__file__).parent
for name, member in (("job-resource.jar", "fabricqueryr-resource.txt"),
                     ("job-archive.zip", "marker.txt")):
    with ZipFile(root / name, "w") as archive:
        info = ZipInfo(member, date_time=(2026, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_STORED
        archive.writestr(info, b"fabricqueryr-dependency-73")
