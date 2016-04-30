#!/bin/sh
###############################################################################################
# File:  habitat_tools.sh
#
# Utility script to create a new habitat repo and update the config and configurations
# CloudFormation is not used since some of the actions are not yet supported by CloudFormation
# and it is simple enough that it is more readable and maintainable to use this script.
#
# Run with no arguments to get usage
#
# Author: Ken Robbins, March 2016
#
#   Copyright 2016 Novartis Institutes for BioMedical Research
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
###############################################################################################

# If I need a package, install in the local directory using: pip install <package> -t .
# and make sure to add it to the zip below. 

# Load configuration
region=`python configutils.py region`
bucket=`python configutils.py bucket`
awsAccountId=`python configutils.py awsAccountId`
metafileMode=`python configutils.py metafileMode`
daysBeforeIA=`python configutils.py daysBeforeIA`
daysBeforeGlacier=`python configutils.py daysBeforeGlacier`
daysBeforeExpire=`python configutils.py daysBeforeExpire`
esDomain=`python configutils.py esDomain`
awsAccountId=`python configutils.py awsAccountId`

# Locally defined configuration
functionName="habitatHandler-${bucket}"
zipFile="lambda_deployment.zip"
filesToDeploy="habitat_handler.py esutils.py metadata.py filenamemeta.py objectmeta.py configutils.py \
    metafile.py defaultMetafileParser.py defaultDataBodyParser.py habitatconfig.json \
    secret.py elasticsearch requests urllib3 requests_aws4auth"
lambdaEventHandler="habitat_handler.event_handler"
unittestTif="unittest-sampledata.tif"
unittestJson="unittest-samplemetadata.json"
unittestCsv="unittest-samplemetadata.csv"
unittestCustom="unittest-samplemetadata.custom"

# Utility functions
make_zip()
{
    rm $zipFile
    chmod -R +r $filesToDeploy
    zip -r $zipFile $filesToDeploy
}

add_bucket_notification()
{
    # In the future, we could add a suffix filter too, but I think that it may just be simpler
    # to just say that anything in /data is a data file to be handled.

    if [ $metafileMode = "disable" ]
    then
        prefix="data/"
    elif [ $metafileMode = "written_first" ]
    then
        prefix="data/"
    elif [ $metafileMode = "written_last" ]
    then
        prefix="meta/"
    else
        echo "ERROR in config file. Valid values for metafileMode are: none, written_first, written_last"
        exit 1
    fi

    notifyFile="notification.json"
    cat > $notifyFile << EOF
{
    "LambdaFunctionConfigurations": [
      {
        "Id": "notifyId_1",
        "LambdaFunctionArn": "arn:aws:lambda:${region}:${awsAccountId}:function:${functionName}",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {
                        "Name": "prefix",
                        "Value": "${prefix}"
                    }
                ]
            }
        }
      }
    ]
}
EOF
    echo "aws s3api put-bucket-notification-configuration --bucket $bucket --notification-configuration file://${notifyFile}"
    aws s3api put-bucket-notification-configuration --bucket $bucket --notification-configuration file://${notifyFile}
}

add_bucket_tags()
{
    # TODO KLR: These are just place holder tags. Need to decide what tags to use.
    NameValue="habitat-test"
    ModeValue="placeholder"
    aws s3api put-bucket-tagging --bucket $bucket --tagging "TagSet=[{Key=Name,Value=${NameValue}},{Key=Mode,Value=${ModeValue}}]"
}

add_bucket_lifecycle()
{
    # TODO KLR: If any config values are 0 or missing, omit that section from the JSON
    # Perhaps I should create a python utility to do this instead of cramming it into bash syntax
    lifecycleFile="lifecycle.json"
    cat > $lifecycleFile << EOF
{
  "Rules": [
      {
          "ID": "lifecycle_id1",
          "Prefix": "",
          "Status": "Enabled",

          "Transitions": [
            {
              "Days": ${daysBeforeIA},
              "StorageClass": "STANDARD_IA"
            },
            {
              "Days": ${daysBeforeGlacier},
              "StorageClass": "GLACIER"
            }
          ],

          "Expiration": {
              "Days": ${daysBeforeExpire}
          }
      }
  ]
}
EOF
    aws s3api put-bucket-lifecycle-configuration --bucket $bucket --lifecycle-configuration  file://${lifecycleFile}
}

make_esconfig()
{
    # Creates an Elasticsearch Service configuration file
    # Parameters:
    # $1 file name for output configuration file
    # Adjust values as necessary
    # TODO KLR: Probably should make this a template file instead
    cat > $1 << EOF
{
    "ElasticsearchClusterConfig": {
        "InstanceType": "t2.micro.elasticsearch",
        "InstanceCount": 1, 
        "DedicatedMasterEnabled": false, 
        "ZoneAwarenessEnabled": false
    }, 
    "EBSOptions": {
        "EBSEnabled": true, 
        "VolumeType": "standard", 
        "VolumeSize": 10, 
        "Iops": 0
    }, 
    "AccessPolicies": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${awsAccountId}:root\"},\"Action\":\"es:*\",\"Resource\":\"arn:aws:es:us-east-1:${awsAccountId}:domain/${esDomain}/*\"}]}", 
    "SnapshotOptions": {
        "AutomatedSnapshotStartHour": 0
    }, 
    "AdvancedOptions": {
    }
}
EOF
}

add_unittest_objects()
{
    # Note that eventually these objects will expire due to the bucket lifecycle policy
    baseFname="unittest-a1234-15-imager_1234567890"
    unittestData="data/${baseFname}.tif"
    aws s3api put-object --bucket $bucket --key $unittestData --body $unittestTif --metadata s3meta1=metaValue1,s3meta2=metaValue2

    # Metadata files
    aws s3 cp $unittestJson s3://${bucket}/meta/${baseFname}.json
    aws s3 cp $unittestCsv s3://${bucket}/meta/${baseFname}.csv
    aws s3 cp $unittestCustom s3://${bucket}/meta/${baseFname}.custom
}

add_elasticsearch_tags()
{
    # TODO KLR: Tags are just place holders for now
    arn="arn:aws:es:${region}:${awsAccountId}:domain/$esDomain"
    aws es add-tags --arn $arn --tag-list 'Key=Name,Value=habitat-test,Key=Environment,Value=dev'
}

# Execute function based on command line argument
option="${1}" 
case ${option} in 
    createbucket) echo "Creating s3 bucket"
        aws s3 mb s3://${bucket} --region $region
        add_unittest_objects
        add_bucket_notification
        add_bucket_tags
        add_bucket_lifecycle
      ;;

    updatebucket) echo "Updating s3 bucket"
        add_unittest_objects
        add_bucket_notification
        add_bucket_tags
        add_bucket_lifecycle
      ;;

    createcode) echo "Creating Lambda function"
        # Do this only once. After created, use updatecode or updateconfig to change (or delete the lambda function)
        make_zip
        aws lambda create-function \
            --region us-east-1 \
            --function-name $functionName \
            --zip-file fileb://${zipFile} \
            --role arn:aws:iam::${awsAccountId}:role/lambda_s3_exec_role  \
            --handler $lambdaEventHandler \
            --runtime python2.7 \
            --timeout 30 \
            --description "Register object when a new S3 object is found" \
            --memory-size 128

        # NB. This needs to occur before add_bucket_notification
        aws lambda add-permission --function-name $functionName --statement-id "perm_id1" --action "lambda:InvokeFunction" \
            --principal s3.amazonaws.com --source-arn "arn:aws:s3:::${bucket}" --source-account $awsAccountId
      ;; 

    updatecode) echo "Updating Lambda function code"
        make_zip
        aws lambda update-function-code \
            --function-name $functionName \
            --zip-file fileb://${zipFile}
      ;; 

    publishcode) echo "Publishing Lambda function"
        make_zip
        aws lambda update-function-code \
            --publish \
            --function-name $functionName \
            --zip-file fileb://${zipFile}
      ;; 

    updatecodeconfig) echo "Updating Lambda function configuration attributes"
        #Only use this as needed and for the attributes that need to change (care with trailing backslashes)
        aws lambda update-function-configuration \
         --function-name $functionName \
         --role arn:aws:iam::${awsAccountId}:role/lambda_s3_exec_role  \
         --handler $lambdaEventHandler \
         --timeout 30 \
         --description "Register object when a new S3 object is found" \
         --memory-size 128
      ;; 

    createesdomain) echo "Creating ElasticSearch domain"
        esconfigFile="esconfig.json"
        make_esconfig $esconfigFile
        aws es create-elasticsearch-domain --domain-name $esDomain --cli-input-json file://${esconfigFile}
        echo "Adding tags to ElasticSearch domain"
        add_elasticsearch_tags

        endpointFound=0
        until [ $endpointFound -eq 1 ]
        do
            python -m esutils putEndpointInConfigFile
            if [ $? = 0 ]
            then
                endpointFound=1
                echo "Elasticsearch domain is now created."
            else
                echo "Elasticserach domain is not ready. Sleeping for 60 seconds..."
                sleep 60
            fi
        done 
      ;; 

    updateesdomain) echo "Updating ElasticSearch domain"
        echo "*** Not completely implemented yet"
        # TODO KLR: flesh out
        # aws es update-elasticsearch-domain-config
        echo "Adding tags to ElasticSearch domain"
        echo "TODO KLR: Can this be done right away or do I need to wait?"
        add_elasticsearch_tags
      ;; 

    testdata) echo "Testing by depositing a data file only..."
        now=`date  "+%Y%d%m%H%M%S"`
        fname="unittest-a1234-15-imager_${now}.tif"
        aws s3 cp $unittestTif s3://${bucket}/data/${fname}
        echo "Pausing for 3 seconds to be sure that Lambda function is complete"
        sleep 3
        echo "Performing an ElasticSearch query to verify this worked."
        # Assumes objectId is created as follows
        objectId=${bucket}/data/${fname}
        python -m esutils getcli $objectId
      ;; 

    testmeta) echo "Testing by depositing a metedata file and then a data file..."
        # Assumes metafileMode in configuration is set to written_first (or none)
        now=`date  "+%Y%d%m%H%M%S"`
        fname="unittest-a1234-15-imager_${now}.tif"
        aws s3 cp $unittestTif s3://${bucket}/data/${fname}
        metafname="unittest-a1234-15-imager_${now}.json"
        aws s3 cp unittest-samplemetadata.json s3://${bucket}/meta/${metafname}
        echo "Pausing for 3 seconds to be sure that Lambda function is complete"
        sleep 3
        echo "Performing an ElasticSearch get to verify this worked."
        # Assumes objectId is created as follows
        objectId=${bucket}/data/${fname}
        python -m esutils getcli $objectId
      ;; 

    deleteall) echo "*** About to delete everything!!! ***"
        echo "*** Proceeding will delete:"
        echo "    !!! S3 bucket $bucket and all of its contents !!!"
        echo "    !!! Lambda function $functionName !!!"
        echo "    !!! ElasticSearch domain $esDomain !!!"
        read -p "To complete this step and delete all of this, type the words: I want to delete everything: " -r
        echo
        if [[ $REPLY == "I want to delete everything" ]]
        then
            echo "Deleting s3 bucket..."
            aws s3 rb s3://${bucket} --force # This won't work if the bucket contains versioned objects
            echo "Deleting Lambda function..."
            aws lambda delete-function --function-name $functionName
            echo "Deleting ElasticSearch domain..."
            aws es delete-elasticsearch-domain --domain-name $esDomain
            echo "Deletion complete."
        else
            echo "Deletion aborted. Phew, that was a close one."
        fi
      ;;

    *) echo "`basename ${0}`:usage: [command]" 
      echo "  Commands:"
      echo "    createcode | updatecode | updatecodeconfig | publishcode"
      echo "    createbucket | updatebucket"
      echo "    createesdomain | updateesdomain"
      echo "    testdata | testmeta"
      echo "    deleteall" 
      echo "  Typical sequence: createesdomain, <wait 15 minutes for domain to be created>, createcode, createbucket, testdata"
      exit 1
      ;; 
esac 
