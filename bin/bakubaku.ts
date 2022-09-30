#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { BakubakuStack } from '../lib/bakubaku-stack';

const app = new cdk.App();
new BakubakuStack(app, 'BakubakuStack', {});
