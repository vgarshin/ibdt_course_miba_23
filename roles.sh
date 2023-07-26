# read list of federated users from file
# and setting or removing roles for folder

# declare roles for E2E course
declare -a ROLES=(
  "viewer"
  "compute.admin"
  "vpc.publicAdmin"
  "iam.serviceAccounts.admin"
  "iam.serviceAccounts.keyAdmin"
  "datalens.instances.user")

# declare common roles for E2E course
# these roles are needed to use network
# in the common folder
declare -a COMMONROLES=(
  "vpc.publicAdmin"
  "vpc.user")

# flag "add" or "remove"
FLAG="remove"

# flag to "create" folders
FOLDER="no"

# name of common folder
COMMONFOLDER="common"

# iterate over users
while read lineuser; do
  echo ============================================================
  userarr=($lineuser)

  # get user id
  output=($(yc organization-manager user list --organization-id bpfldhk2bssh1eebr06j | grep ${userarr[2]}))

  # list user's info
  echo user name ${userarr[0]}
  echo user surname ${userarr[1]}
  echo user email ${userarr[2]}
  echo user folder ${userarr[2]%@*}
  echo user id ${output[1]}
  echo current roles in ${userarr[2]%@*}
  yc resource-manager folder list-access-bindings ${userarr[2]%@*}

  # create folder if necssary
  if [ ${FOLDER} = "create" ]
  then
    descr="${userarr[0]} ${userarr[1]}"
    yc resource-manager folder create \
      --name ${userarr[2]%@*} \
      --description ${descr}
  fi

  # iterate over roles and add or remove roles
  # for folder to federated user
  for role in "${ROLES[@]}"
  do
    if [ ${FLAG} = "add" ]
    then
      # adding role
      echo setting role ${role}
      yc resource-manager folder add-access-binding ${userarr[2]%@*} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    elif [ ${FLAG} = "remove" ]
    then
      # removing role
      echo removing role ${role}
      yc resource-manager folder remove-access-binding ${userarr[2]%@*} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    else
      echo "no action"
    fi
  done

  # iterate over common roles and add or remove
  # roles for common folder to federated user
  for role in "${COMMONROLES[@]}"
  do
    if [ ${FLAG} = "add" ]
    then
      # adding role
      echo setting common role ${role} in folder ${COMMONFOLDER}
      yc resource-manager folder add-access-binding ${COMMONFOLDER} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    elif [ ${FLAG} = "remove" ]
    then
      # removing role
      echo removing common role ${role} in folder ${COMMONFOLDER}
      yc resource-manager folder remove-access-binding ${COMMONFOLDER} \
        --role ${role} \
        --subject federatedUser:${output[1]}
    else
      echo "no action"
    fi
  done

  echo new roles in ${userarr[2]%@*}
  yc resource-manager folder list-access-bindings ${userarr[2]%@*}

done < roles.txt
