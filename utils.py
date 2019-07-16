__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.7.0"



def split_to_sublists(mylist, length, parts):
        s, r = divmod(length, parts)
        k = s+1
        return [mylist[i:i+k] for i in range(0, r*k, k)] + [mylist[i:i+s] for i in range(r*k, length, s)]
