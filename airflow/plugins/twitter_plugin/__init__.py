from airflow.plugins_manager import AirflowPlugin
from twitter_plugin.hooks.twitter_hook import TwitterHook
from twitter_plugin.operators.tweets_to_s3_operator import TweetsToS3Operator


class TwitterPlugin(AirflowPlugin):
    name = "twitter_plugin"
    # A list of class(es) derived from BaseOperator
    operators = [TweetsToS3Operator]
    # A list of class(es) derived from BaseHook
    hooks = [TwitterHook]
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
