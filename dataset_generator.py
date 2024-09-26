import json
import numpy as np


class ChatDataLoader(object):
    def __init__(
        self,
        # assuming normal distribution
        mean_concurrent_users,  # number of concurrent users
        deviation_concurrent_users,
        mean_read_rate,  # rate for reading
        deviation_read_rate,
        mean_type_rate,  # rate of typing
        deviation_type_rate,
    ):

        self.mean_concurrent_users = mean_concurrent_users
        self.deviation_concurrent_users = deviation_concurrent_users
        self.mean_read_rate = mean_read_rate
        self.deviation_read_rate = deviation_read_rate
        self.mean_type_rate = mean_type_rate
        self.deviation_type_rate = deviation_type_rate

        # distribution shift
        self.normal_distribution = np.random.default_rng()
        # this is potentially wrong need to switch to random int within a min and max
        self.num_current_clients = int(
            self.normal_distribution.normal(
                self.mean_concurrent_users, self.deviation_concurrent_users
            )
        )
        # numbers of word read
        self.words_read_per_minute = self.normal_distribution.normal(
            self.mean_read_rate, self.deviation_read_rate, size=self.num_current_clients
        )
        # number of words typed
        self.words_types_per_minute = self.normal_distribution.normal(
            self.mean_type_rate, self.deviation_type_rate, size=self.num_current_clients
        )

        with open("/users/TA744/sharegpt90k/sg_90k_part1.json", "r") as fopen:
            self.open_data = json.load(fopen)
        # a list which contains active connections
        self.active_sessions = list()
        self.time_delta_next_req = dict()
        self.client_id = 0
        return None

    def calculate_time(self):
        """
        ins: the next conversation to send, read speed and type speed
        outs: per conversation time to send next information
        """
        next_send = [self.active_sessions[idx]]
        next_recv = [self.active_sessions.pop(0)]

        return None

    def send_data(self):
        """
        Based on number of users and prompt size decide.
        """

        # get the conversations in the list for client id
        self.active_sessions.extend(
            [
                (self.client_id + i, self.open_data.pop(0)["conversations"])
                for i in range(self.num_current_clients)
            ]
        )

        import ipdb

        ipdb.set_trace()

        # send RPC calls

        # decide when to send the next request
        # the logic for this is  - time to read the recieved output from LLM + time to type next query


if __name__ == "__main__":
    dataloader = ChatDataLoader(10, 10, 10, 10, 10, 10)
    dataloader.send_data()
