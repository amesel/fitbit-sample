import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as timestream from 'aws-cdk-lib/aws-timestream';
import { aws_events, aws_events_targets } from 'aws-cdk-lib';
import { RemovalPolicy } from 'aws-cdk-lib';

export class BakubakuStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // SecretsManager
    const fitbitSecret = secretsmanager.Secret.fromSecretNameV2(this, 'fitbit-secret', 'fitbit/secret');
    const fitbitToken = secretsmanager.Secret.fromSecretNameV2(this, 'fitbit-token', 'fitbit/token');

    // Lambda Layer fitbit
    const fitbit_layer = new lambda.LayerVersion(this, 'FitbitLayer', {
      removalPolicy: RemovalPolicy.RETAIN,
      code: lambda.Code.fromAsset('lambda/fitbit-layer'),
      compatibleArchitectures: [lambda.Architecture.X86_64],
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
    });

    // Lambda Layer common
    const common_layer = new lambda.LayerVersion(this, 'CommonLayer', {
      removalPolicy: RemovalPolicy.RETAIN,
      code: lambda.Code.fromAsset('lambda/common-layer'),
      compatibleArchitectures: [lambda.Architecture.X86_64],
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
    });

    // Lambda Function heartrate
    const fn_heartrate = new lambda.Function(this, 'HeartrateHandler', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda/heartrate'),
      handler: 'lambda_function.lambda_handler',
      environment: {
        CLIENT_ID: fitbitSecret.secretValueFromJson('client_id').unsafeUnwrap(),
        CLIENT_SECRET: fitbitSecret.secretValueFromJson('client_secret').unsafeUnwrap(),
      },
      layers: [fitbit_layer, common_layer],
    });
    fitbitSecret.grantRead(fn_heartrate);
    fitbitToken.grantRead(fn_heartrate);
    fitbitToken.grantWrite(fn_heartrate);

    // EventBridge
    new aws_events.Rule(this, 'HeartrateScheduleRule', {
      schedule: aws_events.Schedule.cron({ minute: '*/15' }),
      targets: [new aws_events_targets.LambdaFunction(fn_heartrate, {})],
    });

    // Lambda Function night
    const fn_night = new lambda.Function(this, 'NightHandler', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda/night'),
      handler: 'lambda_function.lambda_handler',
      timeout: cdk.Duration.minutes(15),
      environment: {
        CLIENT_ID: fitbitSecret.secretValueFromJson('client_id').unsafeUnwrap(),
        CLIENT_SECRET: fitbitSecret.secretValueFromJson('client_secret').unsafeUnwrap(),
      },
      layers: [fitbit_layer, common_layer],
    });
    fitbitSecret.grantRead(fn_night);
    fitbitToken.grantRead(fn_night);
    fitbitToken.grantWrite(fn_night);

    // EventBridge
    new aws_events.Rule(this, 'NightScheduleRule', {
      schedule: aws_events.Schedule.cron({ hour: '0', minute: '5' }), // JST 9:05
      targets: [new aws_events_targets.LambdaFunction(fn_night, {})],
    });

    // Timestream Database
    const cfnDatabase = new timestream.CfnDatabase(this, 'TSDatabase', {
      databaseName: 'bakubaku',
    });
    cfnDatabase.applyRemovalPolicy(RemovalPolicy.RETAIN);

    // Timestream Table
    const cfnTable = new timestream.CfnTable(this, 'TSTable', {
      databaseName: 'bakubaku',
      tableName: 'fitbit',
      retentionProperties: {
        memoryStoreRetentionPeriodInHours: (24 * 30).toString(10),
        magneticStoreRetentionPeriodInDays: (365 * 1).toString(10),
      },
    });
    cfnTable.node.addDependency(cfnDatabase);
    cfnTable.applyRemovalPolicy(RemovalPolicy.RETAIN);

    // IAM
    const timestreamPolicy = new iam.PolicyStatement({
      actions: ['timestream:DescribeEndpoints', 'timestream:WriteRecords'],
      resources: ['*'],
    });
    fn_heartrate.addToRolePolicy(timestreamPolicy);
    fn_night.addToRolePolicy(timestreamPolicy);
  }
}
