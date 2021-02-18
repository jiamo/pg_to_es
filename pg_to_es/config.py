import os
import yaml
import logging
import json


stage = os.environ.get('BINLOG_STAGE', 'develop')


def parse_stage_config(config, stage):
    stage_config = config[stage].copy()
    if 'inherit' in stage_config:
        base_config = parse_config(config, stage_config.pop('inherit'))
        base_config.update(stage_config)
        return base_config

    return stage_config


def parse_config(config, stage):
    stage_config = parse_stage_config(config, stage)

    return stage_config


with open('config.yml') as f:
    config = yaml.load(f.read())

user_config = parse_config(config, stage)
logging.stage = stage

