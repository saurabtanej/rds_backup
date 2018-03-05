#!/usr/bin/env python3
'''
Takes RDS snapshots of all the RDS instances in a region

This script also deletes the snapshots older than 25 days

Input parameters:
- region

Usage:
python3 rds_backup.py --region REGION

'''

import boto3
import datetime
import re
import botocore
from botocore.exceptions import ClientError

DEFAULT_REGION = 'eu-central-1'
DRYRUN = False

def list_rds_instances(client):
    try:
        response = client.describe_db_instances()['DBInstances']
        dbInstances = [i['DBInstanceIdentifier'] for i in response if 'DBInstanceIdentifier' in i]
        return dbInstances
    except botocore.exceptions.ClientError as e:
        raise Exception("Could not list rds instances: %s" % e)

def byTimestamp(snap):
  if 'SnapshotCreateTime' in snap:
    return datetime.datetime.isoformat(snap['SnapshotCreateTime'])
  else:
    return datetime.datetime.isoformat(datetime.datetime.now())

def create_rds_snapshot(client):

    def rds_snapshot_env(instance):
      print("%s RDS snapshot backups started at %s...\n" % (instance, datetime.datetime.now()))
      client.create_db_snapshot(
          DBInstanceIdentifier = instance,
          DBSnapshotIdentifier = "backup-"+instance+'{}'.format(datetime.datetime.now().strftime("%y-%m-%d-%H")),
          Tags = [
              {
                  'Key': 'db_backup',
                  'Value': 'backup'

              },
          ]
      )

    for instance in list_rds_instances(client):
        try:
            latest_snaps = client.describe_db_snapshots(DBInstanceIdentifier = instance)['DBSnapshots']
            latest_snap_time = sorted(latest_snaps, key=byTimestamp, reverse=True)[0]['SnapshotCreateTime'].replace(tzinfo=None)

            if "lab" in instance:
                pass
            elif "prd" in instance:
                rds_snapshot_env(instance=instance)
            elif ((datetime.datetime.now() - datetime.timedelta(hours=12)) > latest_snap_time) and ("prd" not in instance):
                rds_snapshot_env(instance=instance)
            else:
                pass

        except botocore.exceptions.ClientError as e:
            raise Exception("Could not take backup: %s" % e)

def remove_old_snapshots(client):
    # Setting retention period of 25 days
    retentionDate = datetime.datetime.now() - datetime.timedelta(days=25)
    try:
        for instance in list_rds_instances(client):
            #get the latest successful manual snapshot
            source_snaps = client.describe_db_snapshots(DBInstanceIdentifier=instance, SnapshotType='manual')['DBSnapshots']
            latest_succesful_snap = sorted(source_snaps, key=byTimestamp, reverse=True)[0]['DBSnapshotIdentifier']

            for snapshot in client.describe_db_snapshots(DBInstanceIdentifier=instance, MaxRecords=50)['DBSnapshots']:
                if "SnapshotCreateTime" in snapshot:
                    createTs = snapshot['SnapshotCreateTime'].replace(tzinfo=None)
                    #saving the last manual backup from deletion
                    if createTs < retentionDate and snapshot['DBSnapshotIdentifier'] != latest_succesful_snap:
                        print("Deleting snapshot id:", snapshot['DBSnapshotIdentifier'])
                        client.delete_db_snapshot(
                            DBSnapshotIdentifier = snapshot['DBSnapshotIdentifier']
                        )
    except botocore.exceptions.ClientError as e:
        raise Exception("Could not delete snapshot: %s" % e)



if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description='RDS instances Backup')
    parser.add_argument('--region', help="AWS default region", action='store')

    args = parser.parse_args()

    if args.region:
       region = args.region
    else:
       region = DEFAULT_REGION

    rds_client = boto3.client('rds',region_name=region)
    create_rds_snapshot(client=rds_client)
    remove_old_snapshots(client=rds_client)

