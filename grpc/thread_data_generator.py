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
        self.num_current_clients = 10
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
        self.dict_lock = threading.Lock()
        self.session_counter = 0
        self.interval_between_sessions = 3
        return None

    async def rpc_call(self, req_data, client_id, is_last):
        """
        RPC calls for sending requests to the server
        """
        async with grpc.aio.insecure_channel("localhost:50051") as channel:
            
            stub = chat_pb2_grpc.LlmEngineStub(channel)
            input = req_data
            # print("This is the input sent: ",input)
            new_req_id = None
            with self.counter_lock:
                self.request_id_counter += 1
                new_req_id = str(self.request_id_counter)
            print(f"Ready to send request with id {new_req_id}, and session id {client_id} with input {input[:100]}!")
            
            request = chat_pb2.ChatReq(
                prompt=input, request_id=new_req_id, session_id=int(client_id), is_last=is_last
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

        
    async def sleep_time(self, sleep_time):
        await asyncio.sleep(sleep_time)
        
    def run_thread_loop(self, loop, rank_client):
        # try:
        #     result = await asyncio.wait_for(long_running_task(), timeout=5.0)
        #     print("Task completed successfully:", result)
        # except asyncio.TimeoutError:
        #     print("Task timed out after 5 seconds.")
        input_data = None
        interval_chat_req = 0
        interval_info_req = 0
        cur_client_id = rank_client
        is_last = True
        with self.dict_lock:
            input_data = self.open_data.pop(0)["conversations"]
            cur_client_id = self.session_counter
            self.session_counter +=1
        req_data = input_data.pop(0)
        assert req_data["from"] == "human"
        while True:
            is_last = True
            coro = self.sleep_time(interval_chat_req)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            future.result()
            # req_data = input_data.pop(0)
            # assert req_data["from"] == "human"
            if len(input_data)>1:
                # there are at least two more
                is_last = False
                next_recv = input_data.pop(0)
                assert next_recv["from"] == "gpt"
                next_send = input_data.pop(0)
                assert next_send["from"] == "human"
                interval_chat_req = len(next_send["value"])/self.mean_type_rate
            coro = self.rpc_call(req_data["value"], cur_client_id, is_last=is_last)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            future.result()
            req_data = next_send
            # TODO Need to be changed to match the actually returned token num
            interval_info_req = 5
            # try:
                
            #     next_recv = self.active_sessions[client_id].pop(0)
            #     assert next_recv["from"] == "gpt"
            #     next_send = self.active_sessions[client_id].pop(0)
            #     assert next_send["from"] == "human"
            # except Exception as e:
            #     pass
            # if len(input_data)> 1:
                
                
            
            if(not is_last):
                coro = self.sleep_time(interval_info_req)
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                future.result()
                coro = self.info_req_call(cur_client_id)
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                future.result()
            else:
                # need to handle processing a new list from dict
                with self.dict_lock:
                    input_data = self.open_data.pop(0)["conversations"]
                    cur_client_id = self.session_counter
                    self.session_counter +=1
                req_data = input_data.pop(0)
                assert req_data["from"] == "human"
                coro = self.sleep_time(self.interval_between_sessions)
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                future.result()
            

    async def send_data(self):
        """
        Based on number of users and prompt size decide.
        """
        loop = asyncio.get_running_loop()
        for single_client in range(self.num_current_clients):
            threading.Thread(target=self.run_thread_loop, args=(loop,single_client)).start()

        # get the conversations in the list for client id

        await asyncio.sleep(300)

if __name__ == "__main__":
    dataloader = ChatDataLoader(10, 5, 25, 5, 20, 5)
    asyncio.run(dataloader.send_data())
