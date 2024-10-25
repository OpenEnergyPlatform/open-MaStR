from open_mastr.soap_api.mirror import MaStRMirror
import datetime

# Dump data
now = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
dump_file = f"{now}_open-mastr-mirror.backup"

mastr_refl = MaStRMirror()
mastr_refl.dump(dump_file)
