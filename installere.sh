#! /bin/bash
#
# Install ere.pl
#
###########################################

function USAGE ()
{

echo "
	install.sh [-h] [-d directory ] [-u]

   Installs ere.pl into the target directory.
	Links ere to ere.pl. Copies runre into target
	directory.

	-d specifies the directory in which to install ere.
	If not specified, the directory will be installed
	in /usr/local/bin.

	-u uninstalls ere.pl  The target directory where
	you installed ere in the first place must be
	specified.  The default is /usr/local/bin.

	You must have permission to write into your target
	directory.
";

return;
}

targetdir=/usr/local/bin
install=1

export OPTIND=1
while getopts uhd: arg
do
   case "${arg}" in
      d)
         targetdir=${OPTARG}
      ;;
      u)
         install=0
      ;;
      h)
         USAGE
         exit 0
      ;;
      *)
         USAGE
         exit 1
	esac
done

if [ $install = 1 ]
then
	echo "Installing ere into $targetdir OK? (y/n)"
	read x
	if [ "$x" = "y" -o "$x" = "Y" ]
	then
		cp ./runre $targetdir;
		chmod +x $targetdir/runre;
		cp ./ere.pl $targetdir;
		chmod +x $targetdir/ere.pl;
		rm $targetdir/ere;
		ln -s $targetdir/ere.pl $targetdir/ere;
	else
		echo "OK. Nevermind.";
		exit 0;
	fi
else
	echo "Uninstalling ere from $targetdir OK? (y/n)"
	read x
	if [ "$x" = "y" -o "$x" = "Y" ]
	then
		rm $targetdir/runre;
		rm $targetdir/ere.pl;
		rm $targetdir/ere;
	else
		echo "OK. Nevermind.";
		exit 0;
	fi
fi
