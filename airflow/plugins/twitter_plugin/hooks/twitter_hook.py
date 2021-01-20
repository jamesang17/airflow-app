from airflow.hooks.base_hook import BaseHook
from tweepy import OAuthHandler
from tweepy import AppAuthHandler
import os

class TwitterHook(BaseHook):
    """
    Interact with Twitter API.

    :param auth_type:   authentication type - OAuth1 (user), OAuth2 (app)
    :type auth_type:    int
    """


    def __init__(
            self,
            auth_type = 2,
            *args,**kwargs
    ):
        """
        Create new connection to Twitter API
        and allows you to interact with twitter api and save data to a file.
        You can then use that file with other
        Airflow operators to move the data into another data source
        :param auth_type:     The context which to access the twitter api.
                                valid values are: 1,2
    
        .. note::
            The oauth handler assumes you have the twitter credentials as
            environment variables and will retrieve the values from them.
        """

        self.t = None
        # Default - set to 2
        self.auth_type = auth_type
        self._args = args
        self._kwargs = kwargs

    def get_conn(self):
        """
        Sign into Twitter.
        If we have already signed in, this will just return the original object
        """
        if self.t:
            return self.t

        # Default authentication type is 2 if auth_type is not specified
        if self.auth_type == 1:
            # connect to Twitter with OAuth1 (user context)
            auth = OAuthHandler(os.getenv('TWITTER_CONSUMER_API_KEY'),os.getenv('TWITTER_CONSUMER_API_SECRET_KEY'))
            auth.set_access_token(os.getenv('TWITTER_ACCESS_TOKEN'),os.getenv('TWITTER_ACCESS_TOKEN_SECRET'))
        elif self.auth_type == 2:
            # connect to Twitter with OAuth2 (app context)
            auth = AppAuthHandler(os.getenv('TWITTER_CONSUMER_API_KEY'),os.getenv('TWITTER_CONSUMER_API_SECRET_KEY'))
        else:
            raise ValueError("Invalid auth_type %d . Valid AuthTypes are: 1,2", self.auth_type)

        self.t = auth

        return self.t