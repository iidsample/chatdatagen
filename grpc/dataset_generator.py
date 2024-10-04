import time
import json
import numpy as np

import asyncio
import grpc
import chat_pb2
import chat_pb2_grpc
import threading


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
        self.num_current_clients = abs(
            int(
                self.normal_distribution.normal(
                    self.mean_concurrent_users, self.deviation_concurrent_users
                )
            )
        )
        # numbers of word read
        print(f"Num current clients {self.num_current_clients}")
        self.crps = abs(
            self.normal_distribution.normal(
                self.mean_read_rate,
                self.deviation_read_rate,
                size=self.num_current_clients,
            )
        )
        # number of words typed
        self.wsps = abs(
            self.normal_distribution.normal(
                self.mean_type_rate,
                self.deviation_type_rate,
                size=self.num_current_clients,
            )
        )
        # PATH = "/users/TA744/sharegpt90k/sg_90k_part1.json"
        PATH = "/home/anyong/Downloads/sg_90k_part1.json"
        with open(PATH, "r") as fopen:
            self.open_data = json.load(fopen)
        # a list which contains active connections
        self.active_sessions = dict()
        self.next_req_data = dict()
        self.next_req_time = dict()
        self.next_info_req_time = dict()
        self.client_id = 0
        self.task_list = []
        self.request_id_counter = 0
        self.counter_lock = threading.Lock()
        return None

    async def rpc_call(self, req_data, client_id):
        """
        RPC calls for sending requests to the server
        """
        async with grpc.aio.insecure_channel("localhost:50051") as channel:
            stub = chat_pb2_grpc.LlmEngineStub(channel)
            input = req_data["value"]
            # print("This is the input sent: ",input)
            with self.counter_lock:
                self.request_id_counter += 1
                new_req_id = str(self.request_id_counter)
            request = chat_pb2.ChatReq(
                prompt=input, request_id=new_req_id, session_id=client_id
            )
            response = await stub.processChatReq(request)
            print("Receive response: ", response.answer)

    def manage_client_request_end(self, client_id):
        """
        Called once all messages from the client are sent.
        Remove the client from active sessions. Draw new number of clients from the distribution.
        If it's more than existing number of clients we do not add more clients
        Right now for simplicity we draw a new number of clients when a client leaves.
        I am open to ideas how to do it better.
        """

        # Remove client with no more
        del self.active_sessions[client_id]

        # draw new number of clients

        new_num_clients = int(
            self.normal_distribution.normal(
                self.mean_concurrent_users, self.deviation_concurrent_users
            )
        )

        if new_num_clients > self.num_current_clients:
            new_clients_to_add = new_num_clients - self.num_current_clients
            new_clients = {
                self.client_id + i: self.open_data.pop(0)["conversations"]
                for i in range(new_clients_to_add)
            }
            self.client_id += new_clients_to_add
            new_client_ids = list(new_clients.keys())
            self.active_sessions.update(new_clients)

            # TODO: Send first RPC requests for new clients immedia
            for client_id in new_client_ids:
                task = asyncio.create_task(
                    self.rpc_call(self.active_sessions[client_id].pop(0), client_id)
                )
                self.task_list.append(task)
                # self.rpc_call(self.active_sessions[client_id].pop(0), client_id)

            # TODO: Also find the next
        else:
            # if the number of clients is less than that we don't do anything.

            pass

        return None

    def time_to_next_send(self, client_id):
        """
        ins: the next conversation to send, read speed and type speed
        outs: per conversation time to send next information
        """
        print(f"Client Status {len(self.active_sessions[client_id])}")
        next_recv = None
        next_send = None
        try:
            print(
                "The length of self.active_sessions[client_id]: ",
                len(self.active_sessions[client_id]),
            )
            next_recv = self.active_sessions[client_id].pop(0)
            assert next_recv["from"] == "gpt"
            next_send = self.active_sessions[client_id].pop(0)
            assert next_send["from"] == "human"
        except Exception as e:
            print(f"Exception {e}")
            # no more chat requests for this client
            self.manage_client_request_end(client_id)
            return None

        read_speed = self.crps[client_id]
        type_speed = self.wsps[client_id]

        time_info_request = len(next_recv["value"]) / read_speed
        time_send_request = (len(next_send["value"]) / type_speed) + time_info_request

        self.next_req_data[client_id] = next_send
        self.next_req_time[client_id] = time_send_request
        self.next_req_time[f"{client_id}_info"] = time_info_request
        # self.next_info_req_time[client_id] = time_info_request
        return None

    def subtract_time_dict(self, min_time):
        """
        Modify dictionary time
        """

        for key in self.next_req_time:
            self.next_req_time[key] -= min_time

        return None

    async def info_req_call(self, client_id):
        async with grpc.aio.insecure_channel("localhost:50051") as channel:
            stub = chat_pb2_grpc.LlmEngineStub(channel)
            # input = req_data["value"]
            # print("This is the input sent: ",input)
            
            request = chat_pb2.InfoReq(session_id=int(client_id))
            response = await stub.processInfoReq(request)
            print("Receive response: ", response.success)

    def blocking_sleep(self, sleep_time):
        time.sleep(sleep_time)

    async def send_data(self):
        """
        Based on number of users and prompt size decide.
        """

        # get the conversations in the list for client id

        self.active_sessions = {
            self.client_id + i: self.open_data.pop(0)["conversations"]
            for i in range(self.num_current_clients)
        }

        self.client_id += self.num_current_clients

        # send RPC calls for all the new clients immediately,
        for client_id in self.active_sessions.keys():
            # self.next_req_data[client_id] = self.active_sessions[client_id].pop(0)
            task = asyncio.create_task(
                self.rpc_call(self.active_sessions[client_id].pop(0), client_id)
            )
            self.task_list.append(task)
            # self.rpc_call(self.active_sessions[client_id].pop(0), client_id)

        # convert dict keys to list to avoid runtime error
        for client_id in list(self.active_sessions):
            if self.active_sessions.get(client_id):
                self.time_to_next_send(client_id)

        # these requests will go without prior information requests
        import ipdb

        ipdb.set_trace()
        while True:
            # find minimum time to send the request
            min_time_client = min(self.next_req_time, key=self.next_req_time.get)
            min_time = self.next_req_time[min_time_client]
            await asyncio.to_thread(self.blocking_sleep, min_time)
            if "info" in min_time_client:
                task = asyncio.create_task(
                    self.info_req_call(min_time_client[:-5])
                )
                self.task_list.append(task)
            else:
                task = asyncio.create_task(
                    self.rpc_call(self.next_req_data[min_time_client], min_time_client)
                )
                self.task_list.append(task)

            del self.next_req_data[min_time_client]
            del self.next_req_time[min_time_client]
            # subtract min time from each other
            self.subtract_time_dict(min_time)
            # find next time for the same client
            self.time_to_next_send(min_time_client)

        # decide when to send the next request
        # the logic for this is  - time to read the recieved output from LLM + time to type next query


if __name__ == "__main__":
    dataloader = ChatDataLoader(10, 10, 10, 10, 10, 10)
    asyncio.run(dataloader.send_data())
