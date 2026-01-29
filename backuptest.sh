#!/bin/sh
##
## HPS some modifications Oct 2014
## scheduledBackup.sh
## scheduledBackup2.sh -db dbname -b /datadev/archive01 -s DailyBkups -c
## scheduledBackup2.sh -db dbname -b /datadev/archive01 -s DailyBkups -gzip 1
## scheduledBackup2.sh -db dbname -b /datadev/archive01 -s DailyBkups -move moveto /data1/offload -a 2 -o 7
## scheduledBackup2.sh -db dbname -b /datadev/archive01 -s DailyBkups -move moveto /data1/offload -scp scpto db2inst1@127.0.0.1/remote/offload -a 2 -o 7
## scheduledBackup2.sh -db dbname -b /datadev/archive01 -s DailyBkups -move -scp -a 2 -o 7
## scheduledBackup2.sh -db dbname -s DailyBkups -move -scp -a 2 -o 7
## scheduledBackup2.sh -db dbname -nobackup -s DailyBkups -move -scp -a 2 -o 7
## sh scheduledBackup2.sh -db xxxx -s DailyBkups -gzip 6 -nobackup -move -movefrom /aaaa/zzzz -moveto /xxxx/yyy -scp -scpfrom /aaaa/xxxxx -scpto xxx@127.0.0.01:/backup1 -a 10 -o 15 -c
## The movefrom = sourcedir directory
## movefrom and scpfrom could be same directory
## RMD changed mtime to mmin and added a lag - Dec 2022
## In November 2023, Victoriano Dominguez updated the script to make it work for multi-directory DB backups and for any customer.

UsageHelp()
{

	echo "Script to take online database backups"
	echo ""
	echo " Note 1: the db backup directory eg /data/archive_01/dbname/DailyBkups is dynamically constructed as follows:"
	echo " the AUTO_BACKUP_DIR param in ~/DBscriptConfig eg /data/archive_01"
	echo " the lowercase DBNAME eg dbname "
	echo " the -s subdirectory command line parameter eg DailyBkups"
	echo ""
	echo " Note 2:  the only functionalities that work on the multi directory mode are as follows:  DB backup, gzipping files, and removing files."
	echo ""
	echo "Usage: ${0} [options]"
	echo " where [options] is one of the following:"
	echo "         -h:	displays this usage screen"
	echo "        -db:	dbname, default is all cataloged databases"
	echo ""
	echo "         -b:	backup directory [param is taken from ~/DBscriptConfig] and the dir must exist"
	echo "         -s:	subdirectory eg DailyBkups / weekly"
	echo "         -c:	use db2 compression on db2 backup"
	echo ""
	echo "  -nobackup:	do not backup db"
	echo ""
	echo "      -move:	move files to offload backup dir"
#	echo "  -movefrom:	directory to move files from eg /data1/from"
	e:cho "    -moveto:	directory to move files to eg /data1/to"
	echo ""
	echo "       -scp:	scp files to another server offload backup dir"
	echo "   -scpfrom:	directory to scp files from eg /data1/from"
	echo "     -scpto:	directory to scp files to eg db2inst1@10.11.12.13:/backup1/serverx"
	echo ""
	echo "      -gzip:	gzip files over number of days"
	echo "    -remove:	age of files to remove, from default backup directory"
	echo "         -a:	age of backups to move/scp to offload backup dir/server, default is more than 7 days"
	echo "         -o:	age of backups to remove on offload backup dir/server, default is more than 7 days"
	echo ""
	echo "  -multidir:	special backup for multi-directory backups"
	echo "-mdcustname:	short name of customer for multi-directory backups"
	echo " -mdnumdirs:	number of directories for multi-directory backups"
	echo ""
	echo "Examples:"
	echo " 1. ${0} -h"
	echo " 2. ${0} -db dbname -b /data/archive01 -s weekly -c"
	echo " 3. ${0} -db dbname -s DailyBkups -move -moveto /dataqa/tmp -a 5 -o 30"
	echo " 4. ${0} -db dbname -s DailyBkups -nobackup -scp -scpto db2inst1@10.11.12.13:/backup1/serverx -a 99 -o 3"
	echo " 5. ${0} -s DailyBkups -c -scp -a 1 -o 6"
	echo " 6. ${0} -s DailyBkups -c -remove 3 -multidir -mdcustname oms_sample_customer -mdnumdirs 2"
	echo ""

}

##
## 	logging function
##	can trap message types, eg sendmail / logger if TYPE=Error
##
function log {

	TYPE=$1
	MSG=$2

	DATE=`date`

	# TYPE:
	# 0 = Error
	# 1 = Warning
	# 3 = Info
	# 5 = Debug
	if [ ${TYPE} -eq 0 ]; then
		TYPEMSG="Error"
	elif [ ${TYPE} -eq 1 ]; then
		TYPEMSG="Warning"
	elif [ ${TYPE} -eq 3 ]; then
		TYPEMSG="Info"
	elif [ ${TYPE} -eq 5 ]; then
		TYPEMSG="Debug"
	else
		TYPEMSG="Other"
	fi

	printf "${DATE} ${TYPEMSG}: ${MSG}\n"

}


##
## function to check if a string is numeric
##
isNumeric()
{
	echo $1 | grep -E '^[0-9]+$' > /dev/null

	return $?

}

##
## read in DBscriptConfig
##
. ~/DBscriptConfig

## init
SCRIPT_NAME=$(basename $0)
SCRIPT_DIR=$(dirname $0)
WHOAMI=$(whoami)
HOSTNAME=$(hostname)
BACKUP_DIR=$AUTO_BACKUP_DIR
COMPRESS=""
REMOVE_FLAG=0
REMOVE_DAYS=7
AGE_OF_BACKUP_TO_MOVE=7
AGE_TO_REMOVE_ON_OFFLOAD=7
NOBACKUP=0
MOVE=0
SCP=0
GZIP=0
GZIP_DAYS=7
CARTER=0


###  Init for multi-directory backups.  Begin:  ###

MULTI_DIR_BKUP_VAR=0
CUSTOMER_SHORTNAME_VAR=""
NUMBER_BACKUP_DIRS_VAR=1

###  Init for multi-directory backups.  End.    ###


## user check
if [ $WHOAMI == "root" ]; then
	echo "Error: This script should be not run as '$WHOAMI', but as instance owner."
	exit 1
fi

##
## command line arguments
##
while [ $# -gt 0 ]
do
	case $1 in
		-h|-H|-help|--help)		UsageHelp; exit 1 ;;

		-db) 	shift; [ ! -z $1 ] && DB=$( echo $1 | tr '[a-z]' '[A-Z]' ) || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;
		-b) 	shift; [ ! -z $1 ] && BACKUP_DIR=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;
		-s)		shift; [ ! -z $1 ] && SUBDIR=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;

		-c)		COMPRESS="COMPRESS" ;;
		-gzip)	GZIP=1 ; shift; isNumeric $1 && { GZIP_DAYS=$1; } || { echo "Error: Must enter an numeric argument for this option"; UsageHelp; exit 1 ; } ;;

		-nobackup)	NOBACKUP=1 ;;

		-move)		MOVE=1 ;;
#		-movefrom) 	shift; 	[ ! -z $1 ] && MOVEFROM=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;
		-moveto) 	shift; 	[ ! -z $1 ] && MOVETO=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;

		-scp)		SCP=1 ;;
		-scpfrom)	shift; [ ! -z $1 ] && SCPFROM=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;
		-scpto)		shift; [ ! -z $1 ] && SCPTO=$1 || { echo "Error: Must enter an argument for this option"; UsageHelp; exit 1 ; } ;;

		-remove)	REMOVE_FLAG=1;
					shift; isNumeric $1 && { REMOVE_DAYS=$1; } || { echo "Error: Must enter an numeric argument for this option"; UsageHelp; exit 1 ; } ;;
		-a)	shift; isNumeric $1 && { AGE_OF_BACKUP_TO_MOVE=$1; } || { echo "Error: Must enter an numeric argument for this option"; UsageHelp; exit 1 ; } ;;
		-o)	shift; isNumeric $1 && { AGE_TO_REMOVE_ON_OFFLOAD=$1; } || { echo "Error: Must enter an numeric argument for this option"; UsageHelp; exit 1 ; } ;;


		-multidir)		MULTI_DIR_BKUP_VAR=1 ;;

		-mdcustname) 	shift; [ ! -z $1 ] && CUSTOMER_SHORTNAME_VAR=$( echo $1 | tr '[a-z]' '[A-Z]' ) || { echo "Error: Must enter an argument for this option (mdcustname)"; UsageHelp; exit 1 ; } ;;

		-mdnumdirs) 	shift; [ ! -z $1 ] && NUMBER_BACKUP_DIRS_VAR=$1 || { echo "Error: Must enter an argument for this option (mdnumdirs)"; UsageHelp; exit 1 ; } ;;


		(-*)    echo "$0: error - unrecognized option $1" 1>&2; exit 1;;
		(*)     break;;
	esac

    shift

done


DEBUG=0
if [ $DEBUG -eq 1 ]; then
	##
	## flags
	##
	echo "db=$DB"
	echo "BACKUP_DIR=$BACKUP_DIR"
	echo "SUBDIR=$SUBDIR"
	echo "COMPRESS=$COMPRESS"
	echo "GZIP=$GZIP, GZIP_DAYS=$GZIP_DAYS"
	echo "NOBACKUP=$NOBACKUP"
	#echo "MOVE=$MOVE,MOVEFROM=$MOVEFROM,MOVETO=$MOVETO"
	echo "SCP=$SCP,SCPFROM=$SCPFROM,SCPTO=$SCPTO"
	echo "MOVE=$MOVE,MOVETO=$MOVETO"
	#echo "SCP=$SCP,SCPTO=$SCPTO"
	echo "REMOVE_FLAG=$REMOVE_FLAG, REMOVE_DAYS=$REMOVE_DAYS"
	echo "AGE_OF_BACKUP_TO_MOVE=$AGE_OF_BACKUP_TO_MOVE"
	echo "AGE_TO_REMOVE_ON_OFFLOAD=$AGE_TO_REMOVE_ON_OFFLOAD"
fi


###  Verification of inputs for multi-directory backups.  Begin:  ###

RC_VAR=0


if [ ${MULTI_DIR_BKUP_VAR} -eq 1 ]; then

	if [ -z "${CUSTOMER_SHORTNAME_VAR}" ]; then

		log 0 "The customer short name input (-mdcustname) cannot be null."
		UsageHelp
		exit 1

	fi

fi

if [ -z "${NUMBER_BACKUP_DIRS_VAR}" ]; then

	log 0 "The input of number of directories for multi-directory backups (-mdnumdirs) must be an integer value larger than 0."
	UsageHelp
	exit 1

else

	isNumeric ${NUMBER_BACKUP_DIRS_VAR}
	RC_VAR=$?

	if [ ${RC_VAR} -ne 0 ]; then

		log 0 "The input of number of directories for multi-directory backups (-mdnumdirs) must be an integer value larger than 0."
		UsageHelp
		exit 1

	fi

	if [ ${NUMBER_BACKUP_DIRS_VAR} -lt 1 ]; then

		log 0 "The input of number of directories for multi-directory backups (-mdnumdirs) must be an integer value larger than 0."
		UsageHelp
		exit 1

	fi

fi

###  Verification of inputs for multi-directory backups.  End.  ###


##
## verification of some input values
##

if [ ! -d $BACKUP_DIR ]; then
	log 0 "Target base directory $BACKUP_DIR does not exist."
	exit 1
fi

#if [ "$COMPRESS" == "COMPRESS" ] && [ $GZIP -eq 1 ]; then
#	log 0 "Can't do both types of compression at the same time. See help."
#	exit 1
#fi

if [ $MOVE -eq 1 ]; then
	if [ -z $MOVETO ]; then
		log 0 "Please verify MOVETO param. See help."
		exit 1
	elif [ ! -z $MOVETO ] && [ ! -d $MOVETO ]; then
		log 0 "The directory $MOVETO does not exist"
		exit 1
	fi
fi

if [ $SCP -eq 1 ]; then
	if [ ! -z $SCPFROM ] && [ ! -d $SCPFROM ]; then
		log 0 "The directory $SCPFROM does not exist"
		exit 1
	fi
 	if [ -z $SCPTO ]; then
		log 0 "SCP flag is set and SCPTO param does not have a value. See help."
		exit 1
	elif [ ! -z $SCPTO ]; then

		## basic regex check
		RC=$( echo $SCPTO | grep '\w@' | grep -q ':' )
		rc=$?
		if [ $rc -eq 0 ]; then

			## extract user@ip and folder
			SCP_HOSTNAME=$( echo $SCPTO | cut -d ':' -f1 )
			SCP_HOSTNAME_DIR=$( echo $SCPTO | cut -d ':' -f2 )

			## verify we can ssh to remote machine
			ssh $SCP_HOSTNAME "exit 0"
			rc=$?
			if [ $rc -ne 0 ]; then
				log 0 "Please verify $SCPTO. Can't ssh to target machine, $SCP_HOSTNAME"
				exit 1
			fi

			## now check the remote folder exists
			ssh $SCP_HOSTNAME "[ -d $SCP_HOSTNAME_DIR ]"
			rc=$?
			if [ $rc -ne 0 ]; then
				log 0 "Please verify SCPTO param. See help. Target machine directory does not exist, $SCP_HOSTNAME_DIR"
				exit 1
			fi
		else
			log 0 "Please verify SCPTO param. See help."
			exit 1
		fi
	fi
fi

# exit


##
## main
##
echo -e "Info: Starting $0 at $(date) on $HOSTNAME\n"

##
## DBNAMES can be read from DBscriptConfig instead of catalog
##
if [ -z "$DBNAMES" ]; then
	DBNAMES=$( db2 list db directory | grep -E "alias|Indirect" | grep -B 1 Indirect | grep alias | awk '{print $4}' | sort )
else
	DBNAMES=$( echo "${DBNAMES}" | tr '[a-z]' '[A-Z]' | sort )
fi

##
## loops for all dbs
##
for DBNAME in $DBNAMES
do

	## just process the one db
	if [ ! -z "$DB" ] && [ "$DB" != "$DBNAME" ] ; then
		continue
	fi

	## can't run script on a STANDBY db
	ROLE=$(db2 get db cfg for $DBNAME | grep 'HADR database role' | cut -d '=' -f2 | sed 's/ *//g')
	if [ -z "$ROLE" ] || [ "$ROLE" == "" ]; then
		log 3 "Can't determine hadr database role from 'db2 get db cfg for $DBNAME'"
		continue
	elif [ "$ROLE" == "STANDBY" ]; then
		log 1 "Can't run script '${0}' for $DBNAME with hadr database role '$ROLE'"
		continue
	fi

	echo -e "\n$DBNAME ...\n"

	## check that we can backup online
	LOGARCHMETH1=$( db2 get db cfg for $DBNAME | grep LOGARCHMETH1 | cut -d= -f2 | sed 's/ *//g' )
	if [ -z "$LOGARCHMETH1" ] || [ "$LOGARCHMETH1" == "" ] || [ "$LOGARCHMETH1" == "OFF" ]; then
		log 1 "Can't do an online backup of $DBNAME, LOGARCHMETH1=$LOGARCHMETH1"
		continue
	fi

	## lower case DBNAME
	dbname=$(echo $DBNAME | tr '[A-Z]' '[a-z]' )

	##
	## BACKUP_DIR/dbname/SUBDIR exist, if not create it and chmod
	##
	DIR=${BACKUP_DIR}/${dbname}/${SUBDIR}
	if [ ! -d $DIR ]; then
  		mkdir -p $DIR
  		if [ $? -ne 0 ]; then
			log 1 "cannot create target dirctory $DIR, for $DBNAME"
			continue
  		fi

		for DIR in "${BACKUP_DIR}/${dbname}" "${BACKUP_DIR}/${dbname}/${SUBDIR}"
		do
			chmod 0755 $DIR
	  		if [ $? -ne 0 ]; then
				log 1 "cannot chmod 0755 target dirctory $DIR for $DBNAME"
				continue
	  		fi
		done
	else
		log 3 "Directory $DIR already exists"
	fi

	## change to target directory
	DIR=${BACKUP_DIR}/${dbname}/${SUBDIR}
	cd $DIR
	if [ $? -ne 0 ]; then
		log 1 "cannot change (cd) to target dirctory $DIR, for db $DBNAME"
		continue
	fi

	log 3 "Current working directory is $DIR"


#########################  Get the list of backup directories for multi-directory backups.  Begin:  #########################

if [ ${MULTI_DIR_BKUP_VAR} -eq 1 ]; then

	MULTI_COUNTER_VAR=1

	DBscriptConfig_PATH_VAR=~/DBscriptConfig

	MULTI_BKUP_DIR_PREFIX_VAR="${CUSTOMER_SHORTNAME_VAR}_AUTO_BACKUP_DIR"
	MULTI_BKUP_DIR_SUFFIX_VAR="0"
	SUFFIX_VAR=""
	CURRENT_BKUP_DIR_VAR=""
	CURRENT_BKUP_DIR_VAL_VAR=""
	MULTI_BKUP_DIR_LIST_VAR=""
	MULTI_BKUP_DIR_ARRY_VAR=""

	DBscrConfig_BKUP_DIRS_VAR=""
	READING_DIR_VAL_FAIL_VAR="false"
	RC_VAR=0


	DBscrConfig_BKUP_DIRS_VAR=$(cat $DBscriptConfig_PATH_VAR | grep "${MULTI_BKUP_DIR_PREFIX_VAR}")
	RC_VAR=$?
	if [ ${RC_VAR} -ne 0 ]; then

		log 0 "issue when reading backup directories from the ${DBscriptConfig_PATH_VAR} file for backuping up of $DBNAME, rc=${RC_VAR}"
		continue

	fi

	if [ -z "${DBscrConfig_BKUP_DIRS_VAR}" ]; then

		log 0 "issue when reading backup directories from the ${DBscriptConfig_PATH_VAR} file for backuping up of $DBNAME, rc=${RC_VAR}"
		continue

	fi


	while [ ${MULTI_COUNTER_VAR} -le ${NUMBER_BACKUP_DIRS_VAR} ]; do

		if [ ${MULTI_COUNTER_VAR} -le 9 ]; then

			SUFFIX_VAR="_${MULTI_BKUP_DIR_SUFFIX_VAR}${MULTI_COUNTER_VAR}"

		else

			SUFFIX_VAR="_${MULTI_COUNTER_VAR}"

		fi

		CURRENT_BKUP_DIR_VAR="${MULTI_BKUP_DIR_PREFIX_VAR}${SUFFIX_VAR}"

		CURRENT_BKUP_DIR_VAL_VAR=""
		CURRENT_BKUP_DIR_VAL_VAR=$(echo "${DBscrConfig_BKUP_DIRS_VAR}" | grep "${CURRENT_BKUP_DIR_VAR}=")
		RC_VAR=$?
		if [ ${RC_VAR} -ne 0 ]; then
			READING_DIR_VAL_FAIL_VAR="true"
			break
		fi

		if [ -z "${CURRENT_BKUP_DIR_VAL_VAR}" ]; then

			READING_DIR_VAL_FAIL_VAR="true"
			break

		fi


		CURRENT_BKUP_DIR_VAL_VAR=${CURRENT_BKUP_DIR_VAL_VAR//[[:space:]]/}
		CURRENT_BKUP_DIR_VAL_VAR=${CURRENT_BKUP_DIR_VAL_VAR//${CURRENT_BKUP_DIR_VAR}=/}


		if [ ! -d "${CURRENT_BKUP_DIR_VAL_VAR}" ]; then

			log 0 "Target backup directory ${CURRENT_BKUP_DIR_VAL_VAR} does not exist."
			exit 1

		fi


		if [ ${MULTI_COUNTER_VAR} -eq 1 ]; then

			MULTI_BKUP_DIR_LIST_VAR="${CURRENT_BKUP_DIR_VAL_VAR}"
			MULTI_BKUP_DIR_ARRY_VAR="${CURRENT_BKUP_DIR_VAL_VAR}"

		else

			MULTI_BKUP_DIR_LIST_VAR="${MULTI_BKUP_DIR_LIST_VAR}, ${CURRENT_BKUP_DIR_VAL_VAR}"
			MULTI_BKUP_DIR_ARRY_VAR="${MULTI_BKUP_DIR_ARRY_VAR} ${CURRENT_BKUP_DIR_VAL_VAR}"

		fi


		let MULTI_COUNTER_VAR=MULTI_COUNTER_VAR+1

	done

	if [ "${READING_DIR_VAL_FAIL_VAR}" = "true" ]; then

		log 0 "issue when reading a backup directory from the ${DBscriptConfig_PATH_VAR} file for backuping up of $DBNAME"
		continue

	fi


	if [ -z "${MULTI_BKUP_DIR_LIST_VAR}" ]; then

		log 0 "issue when reading backup directories from the ${DBscriptConfig_PATH_VAR} file for backuping up of $DBNAME"
		continue

	fi

	if [ -z "${MULTI_BKUP_DIR_ARRY_VAR}" ]; then

		log 0 "issue when reading backup directories from the ${DBscriptConfig_PATH_VAR} file for backuping up of $DBNAME"
		continue

	fi

fi

#########################  Get the list of backup directories for multi-directory backups.  End.  #########################


	##
	## may not want to do a backup
	##
	if [ $NOBACKUP -eq 0 ]; then

		log 3 "Backing up database $DBNAME ..."


		## the actual backup
		## if the backup fails - don't remove previous backups

		if [ ${MULTI_DIR_BKUP_VAR} -eq 1 ]; then
			###  db2 -v backup db $DBNAME online to $DIR, $DIR with 4 buffers buffer 132 parallelism 2 $COMPRESS include logs
			###  db2 -v backup db $DBNAME online to $FINAL_DIRS_VAR with 8 buffers buffer 256 parallelism 8 $COMPRESS include logs
			db2 -v backup db $DBNAME online to ${MULTI_BKUP_DIR_LIST_VAR} $COMPRESS include logs
			rc=$?
		else
			db2 -v backup db $DBNAME online $COMPRESS include logs
			rc=$?
		fi

		if [ $rc -ne 0 ]; then
			log 0 "issue backuping up of $DBNAME in $DIR ,rc=$rc"
			continue
		fi
	fi


	if [ $GZIP -eq 1 ]; then

		if [ ${MULTI_DIR_BKUP_VAR} -eq 1 ]; then

			for DIR_ITER_VAR in $MULTI_BKUP_DIR_ARRY_VAR; do

		        	log 3 "Attempting to gzip files in $DIR_ITER_VAR over $GZIP_DAYS days"

				FINDFILES="$DBNAME*.00?"
				FILES_TO_GZIP=$( find ${DIR_ITER_VAR} -name "$FINDFILES" -mtime $GZIP_DAYS )


				for file in $FILES_TO_GZIP
				do
					log 3 "Attempting to gzip $file"
					gzip $file
					rc=$?
					if [ $rc -eq 0 ]; then
						log 3 " Successfully gzipped $file"
					else
						log 1 " Failed to gzip $file , rc=$rc"
					fi
				done

				log 3 "Finished gzipping files in ${DIR_ITER_VAR}"

			done

		else

			log 3 "Attempting to gzip files in $DIR over $GZIP_DAYS days"

			FINDFILES="$DBNAME*.00?"
			FILES_TO_GZIP=$( find $DIR -name "$FINDFILES" -mtime $GZIP_DAYS )
			for file in $FILES_TO_GZIP
			do
				log 3 "Attempting to gzip $file"
				gzip $file
				rc=$?
				if [ $rc -eq 0 ]; then
					log 3 " Successfully gzipped $file"
				else
					log 1 " Failed to gzip $file , rc=$rc"
				fi
			done

			log 3 "Finished gzipping files in $DIR"

		fi

	fi


	if [ $REMOVE_FLAG -eq 1 ]; then

		if [ ${MULTI_DIR_BKUP_VAR} -eq 1 ]; then

			for DIR_ITER_VAR in $MULTI_BKUP_DIR_ARRY_VAR; do

				log 3 "Attempting to remove files in ${DIR_ITER_VAR} over $REMOVE_DAYS days"

				FINDFILES="$DBNAME*.00?*"
				REMOVE_MINUTES=$(((REMOVE_DAYS * 1440) - 180))
				FILES_TO_REMOVE=$( find ${DIR_ITER_VAR} -name "$FINDFILES" -mmin $REMOVE_MINUTES -o -mmin +$REMOVE_MINUTES )
				for file in $FILES_TO_REMOVE
				do
					log 3 "Attempting to remove $file"
					rm -f $file
					rc=$?
					if [ $rc -eq 0 ]; then
						log 3 " Successfully removed $file"
					else
						log 1 " Failed to remove $file ,rc=$rc"
					fi
				done

				log 3 "Finished removing files in ${DIR_ITER_VAR}"

			done

		else

			log 3 "Attempting to remove files in $DIR over $REMOVE_DAYS days"

			FINDFILES="$DBNAME*.00?*"
			REMOVE_MINUTES=$(((REMOVE_DAYS * 1440) - 180))
			FILES_TO_REMOVE=$( find $DIR -name "$FINDFILES" -mmin $REMOVE_MINUTES -o -mmin +$REMOVE_MINUTES )
			##FILES_TO_REMOVE=$( find $DIR -name "$FINDFILES" -mtime $REMOVE_DAYS -o -mtime +$REMOVE_DAYS )
			for file in $FILES_TO_REMOVE
			do
				log 3 "Attempting to remove $file"
				rm -f $file
				rc=$?
				if [ $rc -eq 0 ]; then
					log 3 " Successfully removed $file"
				else
					log 1 " Failed to remove $file ,rc=$rc"
				fi
			done

			log 3 "Finished removing files in $DIR"

		fi

	fi


	##
	## local offload files
	##
	if [ $MOVE -eq 1 ]; then

		## mkdir the directory if it does not exist
		## LOCAL_OFFLOAD_DIR will be like /backup1/micronstg/dbname/DailyBkups
		LOCAL_OFFLOAD_DIR=${MOVETO}/${dbname}/${SUBDIR}
		if [ ! -d $LOCAL_OFFLOAD_DIR ]; then
			mkdir -p $LOCAL_OFFLOAD_DIR
			rc=$?
			if [ $rc -eq 0 ]; then
				log 3 "Created local dir $LOCAL_OFFLOAD_DIR"
			else
				log 0 "Failed to create local dir $LOCAL_OFFLOAD_DIR ,rc=$rc"
				continue
			fi
		fi

		## mv the files
		FINDFILES="$DBNAME*.00?*"
		BACKUP_DB_FILES=$( find $DIR -name "$FINDFILES" -mtime +$AGE_OF_BACKUP_TO_MOVE )
		NUMFILES=$( find $DIR -name "$FINDFILES" -mtime +$AGE_OF_BACKUP_TO_MOVE | wc -l )
		log 3 "Attempting to move $NUMFILES files in $DIR to $LOCAL_OFFLOAD_DIR ... for db $DBNAME over $AGE_OF_BACKUP_TO_MOVE days"
		for file in $BACKUP_DB_FILES
		do
			mv $file $LOCAL_OFFLOAD_DIR
			rc=$?
			if [ $rc -eq 0 ]; then
				log 3 "moved $file to $LOCAL_OFFLOAD_DIR"
			else
				log 0 "there was an error moving $file to $LOCAL_OFFLOAD_DIR ,rc=$rc"
			fi
		done

		## remove files over a certain age in offload dir
		BACKUP_DB_FILES=$( find $LOCAL_OFFLOAD_DIR -name "$FINDFILES" -mtime $AGE_TO_REMOVE_ON_OFFLOAD )
		NUMFILES=$( find $LOCAL_OFFLOAD_DIR -name "$FINDFILES" -mtime $AGE_TO_REMOVE_ON_OFFLOAD | wc -l )
		log 3 "Attempting to remove $NUMFILES files in $LOCAL_OFFLOAD_DIR ... for db $DBNAME over $AGE_TO_REMOVE_ON_OFFLOAD days"
		for file in $BACKUP_DB_FILES
		do
			# echo $file
			rm -f $file
			rc=$?
			if [ $rc -eq 0 ]; then
				log 3 "removed $file from $LOCAL_OFFLOAD_DIR"
			else
				log 0 "there was an error removing $file from $LOCAL_OFFLOAD_DIR ,rc=$rc"
			fi
		done
	fi

	##
	## scp offload backups
	## validate target DIR exists, create if not
	## scp file and then remove files locally and remotely
	##
	if [ $SCP -eq 1 ]; then

		## SCP_OFFLOAD_DIR will be like /backup1/micronstg/dbname/DailyBkups
		SCP_OFFLOAD_DIR=${SCP_HOSTNAME_DIR}/${dbname}/${SUBDIR}
		ssh $SCP_HOSTNAME "[ -d $SCP_OFFLOAD_DIR ] || mkdir -p $SCP_OFFLOAD_DIR"
		rc=$?
		if [ $rc -eq 0 ]; then
			log 3 "Remote dir $SCP_OFFLOAD_DIR exists on $SCP_HOSTNAME"
		else
			log 0 "Remote dir $SCP_OFFLOAD_DIR does not exist or can't be created on $SCP_HOSTNAME"
			continue
		fi

		##
		## setup SCP_OFFLOAD target dir
		## SCP_TARGET will be like "db2inst1@192.168.14.7:/backup1/micronstg/dbname/DailyBkups"
		##

		##
		## we now may need to overwrite the DIR variable
		##
		if [ ! -z $SCPFROM ]; then
			DIR=$SCPFROM/$dbname/$SUBDIR
			if [ -d $DIR ]; then
				log 3 "Directory $DIR exists"
			else
				log 1 "Directory $DIR does not exist"
				continue
			fi
		fi

		SCP_TARGET=${SCP_HOSTNAME}:${SCP_OFFLOAD_DIR}

		FINDFILES="$DBNAME*.00?*"
		BACKUP_DB_FILES=$( find $DIR -name "$FINDFILES" -mtime $AGE_OF_BACKUP_TO_MOVE )
		NUMFILES=$( find $DIR -name "$FINDFILES" -mtime $AGE_OF_BACKUP_TO_MOVE | wc -l )
		log 3 "Attempting to scp $NUMFILES files in $DIR to $SCP_TARGET ... for db $DBNAME over $AGE_OF_BACKUP_TO_MOVE days"
		for file in $BACKUP_DB_FILES
		do
			## remove the -p
			## scp -p $file $SCP_TARGET
			scp $file $SCP_TARGET
			rc=$?
			if [ $rc -eq 0 ]; then

				log 3 "scpped $file to $SCP_TARGET ok"

				rm -f $file
				rc=$?
				if [ $rc -eq 0 ]; then
					log 3 "removed file $file"
				else
					log 0 "failed to remove file $file ,rc=$rc"
				fi

			else
				log 0 "there was an error scpping $file to $SCP_TARGET ,rc=$rc"
			fi

		done

		##
		## found a bug when doing Newell, where the daily backups were copied to
		## /apps2/usrd11q7730008/aunew01u/dailybackups/ rather than /apps2/usrd11q7730008/aunew01u/DailyBkups
		## I added a sym link to this fodler but it caused "find" issues on the target below
		## need to add a / to the scp offload directory if the offload directory is a sym link
		##
		RC=$( ssh $SCP_HOSTNAME "[ -L $SCP_OFFLOAD_DIR ] && [ -d $SCP_OFFLOAD_DIR ]" )
		rc=$?
		if [ $rc -eq 0 ]; then
			SCP_OFFLOAD_DIR=${SCP_OFFLOAD_DIR}/
		fi

		##
		## remove remote files
		## need to escape $ remote code as \$
		## log is not remote so we need to use echo
		##
		NUMFILES=$( ssh $SCP_HOSTNAME "find $SCP_OFFLOAD_DIR -name "$FINDFILES" -mtime $AGE_TO_REMOVE_ON_OFFLOAD | wc -l" )
		log 3 "Attempting to remove $NUMFILES files in $SCP_TARGET ... for db $DBNAME over $AGE_TO_REMOVE_ON_OFFLOAD days"
		if [ ! -z "$NUMFILES" ] && [ $NUMFILES -gt 0 ]; then
			ssh $SCP_HOSTNAME "
				rc=0
				# for file in \$( find $SCP_OFFLOAD_DIR -name $DBNAME*.00?* -mtime $AGE_TO_REMOVE_ON_OFFLOAD )
				for file in \$( find $SCP_OFFLOAD_DIR -name $FINDFILES -mtime $AGE_TO_REMOVE_ON_OFFLOAD )
				do
					# echo \$file
					rm -f \$file;
					rc=$?
					if [ \$rc -eq 0 ]; then
						echo "Info: removed file \$file";
					else
						echo "Error: failed to remove file \$file ,rc=\$rc" ;
					fi
				done
				exit \$rc; ";

			rc=$?
			if [ $rc -ne 0 ]; then
				log 1 "appears to have been an error removing files for $SCP_HOSTNAME in $SCP_OFFLOAD_DIR"
			fi

		fi

	fi

done

##
## cleanup
##

echo -e "\nInfo: Completed $0 at $(date)"

exit 0
