package mr

import (
	"fmt"
	"time"
)

type TaskType int

const MapType TaskType = 0
const ReduceType TaskType = 1

type Task struct {
	Filename string
	NReduce  int
	NMap     int
	Seq      int
	Type     TaskType
	Active   bool
}

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

func reduceName(mapIndex, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func mergeName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
