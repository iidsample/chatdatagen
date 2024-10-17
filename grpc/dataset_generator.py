import time
import json
import numpy as np
import random
import pdb

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
        self.continue_task = 0.5

        # distribution shift
        self.normal_distribution = np.random.default_rng()
        # this is potentially wrong need to switch to random int within a min and max
        # self.num_current_clients = abs(
        #     int(
        #         self.normal_distribution.normal(
        #             self.mean_concurrent_users, self.deviation_concurrent_users
        #         )
        #     )
        # )
        # simplify for checking correctness and debugging
        self.num_current_clients = 2
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
            new_req_id = None
            with self.counter_lock:
                self.request_id_counter += 1
                new_req_id = str(self.request_id_counter)
            print(f"Ready to send request with id {new_req_id}, and session id {client_id} with input {input[:100]}!")
            
            request = chat_pb2.ChatReq(
                prompt=input, request_id=new_req_id, session_id=int(client_id)
            )
            # pdb.set_trace()
            response = await stub.processChatReq(request)
            print("Receive llm text: ", response.answer[:100])
            # pdb.set_trace()
            
    def thread_rpc_call(self, loop, req_data, client_id):
        # report a message
        # print('thread running')
        # wait a moment
        # create a coroutine
        coro = self.rpc_call(req_data, client_id)
        # execute a coroutine
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        # wait for the task to finish
        future.result()
        # report a message
        # print('thread done')
        
    def thread_info_req(self, loop, client_id):
        coro = self.info_req_call(client_id=client_id)
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        future.result()    

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

        max_key = max(self.active_sessions)
        new_num_clients = int(
            self.normal_distribution.normal(
                self.mean_concurrent_users, self.deviation_concurrent_users
            )
        )

        if new_num_clients > self.num_current_clients:
            new_clients_to_add = new_num_clients - self.num_current_clients
            new_clients = {
                # may be set to max(client_id) + i
                # max_key + i: self.open_data.pop(0)["conversations"]
                self.client_id + i: self.open_data.pop(0)["conversations"]
                for i in range(new_clients_to_add)
            }
            self.client_id += new_clients_to_add
            new_client_ids = list(new_clients.keys())
            self.active_sessions.update(new_clients)

            # TODO: Send first RPC requests for new clients immedia
            loop = asyncio.get_running_loop()
            
            for client_id in new_client_ids:
                threading.Thread(target=self.thread_rpc_call, args=(loop,self.active_sessions[client_id].pop(0), client_id)).start()
                # task = asyncio.create_task(
                #     self.rpc_call(self.active_sessions[client_id].pop(0), client_id)
                # )
                # self.task_list.append(task)
                # self.rpc_call(self.active_sessions[client_id].pop(0), client_id)

            # TODO: Also find the next
        else:
            # if the number of clients is less than that we don't do anything.

            pass

        return None
    
    def manage_client_request_end_add_one(self, client_id):
        """
        Called once all messages from the client are sent.
        Remove the client from active sessions. Draw new number of clients from the distribution.
        If it's more than existing number of clients we do not add more clients
        Right now for simplicity we draw a new number of clients when a client leaves.
        I am open to ideas how to do it better.
        """

        # Remove client with no more
        del self.active_sessions[client_id]

        if(random.random()<self.continue_task):
            new_clients_to_add = 1
            new_clients = {
                # may be set to max(client_id) + i
                # max_key + i: self.open_data.pop(0)["conversations"]
                self.client_id + i: self.open_data.pop(0)["conversations"]
                for i in range(new_clients_to_add)
            }
            self.client_id += new_clients_to_add
            new_client_ids = list(new_clients.keys())
            self.active_sessions.update(new_clients)

            # TODO: Send first RPC requests for new clients immedia
            loop = asyncio.get_running_loop()
            for client_id in new_client_ids:
                threading.Thread(target=self.thread_rpc_call, args=(loop,self.active_sessions[client_id].pop(0), client_id)).start()
                # task = asyncio.create_task(
                #     self.rpc_call(self.active_sessions[client_id].pop(0), client_id)
                # )
                # self.task_list.append(task)
                # self.rpc_call(self.active_sessions[client_id].pop(0), client_id)

            # TODO: Also find the next
            for client_id in list(new_clients):
                if new_clients.get(client_id):
                    self.time_to_next_send(client_id)

        return None

    def time_to_next_send(self, client_id):
        """
        ins: the next conversation to send, read speed and type speed
        outs: per conversation time to send next information
        """
        # print(f"Client Status {len(self.active_sessions[client_id])}")
        next_recv = None
        next_send = None
        try:
            print(
                f"The length of self.active_sessions[{client_id}]: ",
                len(self.active_sessions[client_id]),
            )
            next_recv = self.active_sessions[client_id].pop(0)
            assert next_recv["from"] == "gpt"
            next_send = self.active_sessions[client_id].pop(0)
            assert next_send["from"] == "human"
        except Exception as e:
            print(f"Exception {e}")
            # no more chat requests for this client
            self.manage_client_request_end_add_one(client_id)
            return None

        # read_speed = self.crps[client_id]
        # type_speed = self.wsps[client_id]
        read_speed = self.mean_read_rate
        type_speed = self.mean_type_rate

        time_info_request = len(next_recv["value"]) / read_speed
        print("time to next Info req: ",time_info_request)
        time_send_request = (len(next_send["value"]) / type_speed) + time_info_request
        print("time to next Send req: ",time_send_request)

        self.next_req_data[str(client_id)] = next_send
        self.next_req_time[str(client_id)] = time_send_request
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
            print(f"sending info request with session id {client_id}")
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
        loop = asyncio.get_running_loop()

        # send RPC calls for all the new clients immediately,
        for client_id in self.active_sessions.keys():
            threading.Thread(target=self.thread_rpc_call, args=(loop,self.active_sessions[client_id].pop(0), client_id)).start()
            # self.next_req_data[client_id] = self.active_sessions[client_id].pop(0)
            # task = asyncio.create_task(
            #     self.rpc_call(self.active_sessions[client_id].pop(0), client_id)
            # )
            # self.task_list.append(task)
            # self.rpc_call(self.active_sessions[client_id].pop(0), client_id)

        # convert dict keys to list to avoid runtime error
        for client_id in list(self.active_sessions):
            if self.active_sessions.get(client_id):
                self.time_to_next_send(client_id)

        # these requests will go without prior information requests
        # import ipdb

        # ipdb.set_trace()
        while True:
            # find minimum time to send the request
            if(len(self.next_req_time)==0 and len(self.next_req_data)==0):
                break
            min_time_client = min(self.next_req_time, key=self.next_req_time.get)
            min_time_client = str(min_time_client)
            min_time = self.next_req_time[min_time_client]
            # print(f"time before calling blocking sleep: {time.time()}, need to sleep for {min_time}")
            # await asyncio.to_thread(self.blocking_sleep, min_time)
            await asyncio.sleep(min_time)
            # print(f"time after calling blocking sleep: {time.time()}")
            if "info" in min_time_client:
                threading.Thread(target=self.thread_info_req, args=(loop, min_time_client[:-5])).start()
                # task = asyncio.create_task(
                #     self.info_req_call(min_time_client[:-5])
                # )
                # self.task_list.append(task)
            else:
                if(self.next_req_data.get(min_time_client)==None):
                    print("Does not found correspondent key in next_req_data, break the loop!")
                    break
                threading.Thread(target=self.thread_rpc_call, args=(loop,self.next_req_data[min_time_client],min_time_client)).start()
                # task = asyncio.create_task(
                #     self.rpc_call(self.next_req_data[min_time_client], min_time_client)
                # )
                # self.task_list.append(task)

            if("info" not in min_time_client):
                del self.next_req_data[min_time_client]
            del self.next_req_time[min_time_client]
            # subtract min time from each other
            self.subtract_time_dict(min_time)
            # find next time for the same client
            if("info" not in min_time_client):
                self.time_to_next_send(int(min_time_client))

        # decide when to send the next request
        # the logic for this is  - time to read the recieved output from LLM + time to type next query


if __name__ == "__main__":
    dataloader = ChatDataLoader(10, 5, 25, 5, 20, 5)
    asyncio.run(dataloader.send_data())
