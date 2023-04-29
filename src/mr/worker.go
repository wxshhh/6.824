package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		var args RequestTaskArgs
		var reply RequestTaskReply

		CallRequestTask(&args, &reply)

		// if there is no task, sleep 1s and re-request
		if reply.Task.ID == -1 {
			time.Sleep(2 * time.Second)
			continue
		}

		//fmt.Println(reply.Task)

		// in-progress
		if reply.Task.TaskType == 0 {
			// map task
			if reply.Task.FileName == "" {
				log.Println("filename is nil")
				time.Sleep(2 * time.Second)
				continue
			}

			file, err := os.Open(reply.Task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v: %v", reply.Task.FileName, err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Task.FileName)
			}
			file.Close()
			kva := mapf(reply.Task.FileName, string(content))
			// split into 10 buckets
			buckets := make(map[int][]KeyValue)
			for _, kv := range kva {
				hash := ihash(kv.Key) % reply.NReduce
				buckets[hash] = append(buckets[hash], kv)
			}
			// sort every bucket
			for _, bucket := range buckets {
				sort.Slice(bucket, func(i, j int) bool {
					return bucket[i].Key < bucket[j].Key
				})
			}

			// save k-v
			//tmpFile := make(map[int]string)
			for i := 0; i < reply.NReduce; i++ {
				pattern := fmt.Sprintf("mr-%d-%d-*.json", reply.Task.ID, i)
				file, err = os.CreateTemp("", pattern)
				if err != nil {
					log.Fatalf("cannot create file: %v\n", err)
				}
				for _, kv := range buckets[i] {
					err = json.NewEncoder(file).Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write file: %v\n", err)
					}
				}
				newPath := fmt.Sprintf("mr-%d-%d.json", reply.Task.ID, i)
				err = os.Rename(file.Name(), newPath)
			}
			//for hash, bucket := range buckets {
			//	pattern := fmt.Sprintf("mr-%d-%d-*.json", reply.Task.ID, hash)
			//	file, err = os.CreateTemp("", pattern)
			//	if err != nil {
			//		log.Fatalf("cannot create file: %v\n", err)
			//	}
			//	err = json.NewEncoder(file).Encode(bucket)
			//	if err != nil {
			//		log.Fatalf("cannot write file: %v\n", err)
			//	}
			//	tmpFile[hash] = file.Name()
			//}

			// rename temp file
			//for i := 0; i < reply.NReduce; i++ {
			//	newPath := fmt.Sprintf("mr-%d-%d.json", reply.Task.ID, i)
			//	err = os.Rename(tmpFile[i], newPath)
			//	if err != nil {
			//		log.Printf("file rename error: %v\n", err)
			//		return
			//	}
			//}

			// tell success
			newTask := reply.Task
			// complete
			CallDoneTask(&DoneTaskArgs{newTask}, &DoneTaskReply{})
		} else {
			// reduce task

			// read json file
			var intermediate []KeyValue
			for i := 0; i < reply.NMap; i++ {
				//var tmpKV []KeyValue
				filePath := fmt.Sprintf("mr-%d-%d.json", i, reply.Task.ID)
				file, err := os.Open(filePath)
				if err != nil {
					log.Printf("%s open error: %v", filePath, err)
					return
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err = dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				//err = json.NewDecoder(file).Decode(&tmpKV)
				//if err != nil {
				//	log.Printf("%s json decode error: %v", filePath, err)
				//	return
				//}
				file.Close()
			}

			// sort keys
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})

			tempFile, err := os.CreateTemp("", "mr-out-*")
			if err != nil {
				log.Fatalf("cannot create file: %v\n", err)
			}

			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-i.
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			// rename output file
			oname := fmt.Sprintf("mr-out-%d", reply.Task.ID)
			err = os.Rename(tempFile.Name(), oname)
			if err != nil {
				log.Printf("file rename error: %v\n", err)
				return
			}

			// tell success
			newTask := reply.Task
			// complete
			CallDoneTask(&DoneTaskArgs{newTask}, &DoneTaskReply{})
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallRequestTask(args *RequestTaskArgs, reply *RequestTaskReply) {
	ok := call("Coordinator.RequestTask", args, reply)
	if ok {
		//fmt.Printf("RequestTask success\n")
	} else {
		//fmt.Printf("call failed\n")
	}
	return
}

func CallDoneTask(args *DoneTaskArgs, reply *DoneTaskReply) {
	ok := call("Coordinator.DoneTask", &args, &reply)
	if ok {
		//fmt.Printf("DoneTask success\n")
	} else {
		//fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
