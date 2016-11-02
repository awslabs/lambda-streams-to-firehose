#!/usr/bin/python3 -tt
"""Cloudformation JSON generator"""
import re

import boto.s3
from troposphere import (
    GetAtt,
    Join,
    Ref,
    Template,
)
import troposphere.awslambda as awslambda
import troposphere.iam as iam
import troposphere.sns as sns
import troposphere.cloudwatch as cloudwatch

APP_NAME = 'lambda-streams-to-firehose'
RESOURCE_APP_NAME = 'LambdaStreamsToFirehose'


def determine_ub_environment(environment):
    """Returns the unbounce environment based on the environment provided"""
    if (
            environment == 'production' or
            re.match(r'production/', environment)
    ):
        return 'production'
    elif (
            environment == 'integration' or
            re.match(r'integration/', environment)
    ):
        return 'integration'
    return 'development'


def find_code_package(
        package,
        app_version,
        s3_bucket='unbounce-maven-repo',
        s3_region='us-east-1',
        package_extension='.zip'
):
    """Returns an s3 object for the package's code"""
    s3_conn = boto.s3.connect_to_region(s3_region)
    package_prefix = (
        'snapshot/com/unbounce/{}/{}/'.format(package, app_version)
        if app_version.endswith('-SNAPSHOT')
        else 'release/com/unbounce/{}/{}/'.format(package, app_version)
    )
    package_bucket = s3_conn.get_bucket(s3_bucket)
    packages = []
    for s3_key in package_bucket.list(prefix=package_prefix):
        if s3_key.key.endswith(package_extension):
            packages.append(s3_key)
    if not packages:
        raise FileNotFoundError(
            '"{}" with version "{}" could not be found in "{}"'
            .format(package, app_version, s3_bucket)
        )
    return packages[-1]


def get_code(package, app_version):
    """Adds the specified package's code to the template"""
    latest_code = find_code_package(package, app_version)
    return awslambda.Code(
        S3Bucket=latest_code.bucket.name,
        S3Key=latest_code.key,
    )


def generate_cloudformation_json(args):
    """Generates and prints out CloudFormation JSON"""
    template = Template()

    ub_environment = determine_ub_environment(args.environment)

    ##########################################################################
    # Resources
    ##########################################################################

    alarm_topic = template.add_resource(sns.Topic(
        '{}AlarmTopic'.format(RESOURCE_APP_NAME),
        Subscription=[
            sns.Subscription(
                Endpoint=(
                    'https://api.opsgenie.com/v1/json/cloudwatch?apiKey='
                    '8353c0a8-1f13-46e3-8046-6e52056438fd'
                ),
                Protocol='https',
            ),
        ],
    ))

    lambda_streams_to_firehose_code = get_code('lambda-streams-to-firehose', args.app_version)

    lambda_streams_to_firehose_policy = iam.Policy(
        '{}Policy'.format(RESOURCE_APP_NAME),
        PolicyName='{}RolePolicy'.format(RESOURCE_APP_NAME),
        PolicyDocument={
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'AllowLoggingOperations',
                    'Effect': 'Allow',
                    'Action': [
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents',
                    ],
                    'Resource': [
                        Join(
                            '',
                            (
                                'arn:aws:logs:*:*:/aws/lambda/',
                                Ref('AWS::StackName'),
                                '*',
                            ),
                        ),
                    ],
                },
                {
                    'Effect': 'Allow',
                    'Action': [
                        's3:PutObject',
                    ],
                    'Resource': [
                        'arn:aws:s3:::unbounce-reports/client-traffic-reports/{}/*'
                        .format(ub_environment),
                    ],
                },
            ],
        },
    )

    lambda_streams_to_firehose_role = template.add_resource(iam.Role(
        '{}Role'.format(RESOURCE_APP_NAME),
        Path='/',
        AssumeRolePolicyDocument={
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': ['sts:AssumeRole'],
                    'Effect': 'Allow',
                    'Sid': '',
                    'Principal': {
                        'Service': ['lambda.amazonaws.com'],
                    },
                },
            ],
        },
        Policies=[
            lambda_streams_to_firehose_policy,
        ],
    ))

    lambda_streams_to_firehose_function = template.add_resource(awslambda.Function(
        RESOURCE_APP_NAME,
        Code=lambda_streams_to_firehose_code,
        Handler='index.handler',
        Role=GetAtt(lambda_streams_to_firehose_role, 'Arn'),
        Runtime='nodejs',
        Timeout=300,
    ))

    template.add_resource(cloudwatch.Alarm(
        '{}ErrorsMonitor'.format(RESOURCE_APP_NAME),
        AlarmDescription='Alarm if errors >= 2 in 1 minute',
        Namespace='AWS/Lambda',
        MetricName='Errors',
        Dimensions=[
            cloudwatch.MetricDimension(
                Name="FunctionName",
                Value=Ref(lambda_streams_to_firehose_function),
            ),
        ],
        Statistic='Sum',
        Period='60',
        EvaluationPeriods='1',
        Threshold='2',
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        AlarmActions=[Ref(alarm_topic)],
        OKActions=[Ref(alarm_topic)],
    ))

    template.add_resource(cloudwatch.Alarm(
        '{}InvocationsMonitor'.format(RESOURCE_APP_NAME),
        AlarmDescription='Alarm if invocations >= 10/min for 2 minutes',
        Namespace='AWS/Lambda',
        MetricName='Invocations',
        Dimensions=[
            cloudwatch.MetricDimension(
                Name="FunctionName",
                Value=Ref(lambda_streams_to_firehose_function),
            ),
        ],
        Statistic='Sum',
        Period='60',
        EvaluationPeriods='2',
        Threshold='10',
        ComparisonOperator='GreaterThanOrEqualToThreshold',
        AlarmActions=[Ref(alarm_topic)],
        OKActions=[Ref(alarm_topic)],
    ))

    template.add_description('{} stack, sans VPC'.format(APP_NAME))

    return template

if __name__ == '__main__':
    import argparse

    ARGS = argparse.ArgumentParser(
        description='Create CloudFormation JSON for {}'.format(APP_NAME),
    )
    ARGS.add_argument(
        '--app-version',
        required=True,
        help='Deploy the specified application version',
    )
    ARGS.add_argument(
        '--environment',
        required=True,
        help='Environment for lp-deployer to deploy',
    )

    ARGS = ARGS.parse_args()

    TEMPLATE = generate_cloudformation_json(ARGS)
    print(TEMPLATE.to_json())
