package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/database/tasks"
)

func GetTasks(c *gin.Context) {
	dbTasks, err := tasks.GetAllTasks()
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve tasks: "+err.Error())
		return
	}
	var responseTasks []gin.H
	for _, t := range dbTasks {
		results, err := tasks.GetTaskResultsByTaskId(t.TaskId)
		if err != nil {
			api.RespondError(c, 500, "Failed to retrieve task results: "+err.Error())
			return
		}

		var filteredResults []gin.H
		for _, r := range results {
			filteredResults = append(filteredResults, gin.H{
				"client":      r.Client,
				"result":      r.Result,
				"exit_code":   r.ExitCode,
				"finished_at": r.FinishedAt,
				"created_at":  r.CreatedAt,
			})
		}

		responseTasks = append(responseTasks, gin.H{
			"task_id": t.TaskId,
			"clients": t.Clients,
			"command": "",
			"results": filteredResults,
		})
	}
	api.RespondSuccess(c, responseTasks)
}

func GetTaskById(c *gin.Context) {
	taskId := c.Param("task_id")
	if taskId == "" {
		api.RespondError(c, 400, "Task ID is required")
		return
	}
	task, err := tasks.GetTaskByTaskId(taskId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve task: "+err.Error())
		return
	}
	if task == nil {
		api.RespondError(c, 404, "Task not found")
		return
	}
	results, err := tasks.GetTaskResultsByTaskId(taskId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve task results: "+err.Error())
		return
	}
	var filteredResults []gin.H
	for _, r := range results {
		filteredResults = append(filteredResults, gin.H{
			"client":      r.Client,
			"result":      r.Result,
			"exit_code":   r.ExitCode,
			"finished_at": r.FinishedAt,
			"created_at":  r.CreatedAt,
		})
	}
	api.RespondSuccess(c, gin.H{
		"task_id": task.TaskId,
		"clients": task.Clients,
		"command": "",
		"results": filteredResults,
	})
}

func GetTasksByClientId(c *gin.Context) {
	clientId := c.Param("uuid")
	if clientId == "" {
		api.RespondError(c, 400, "Client ID is required")
		return
	}
	taskList, err := tasks.GetTasksByClientId(clientId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve tasks: "+err.Error())
		return
	}
	if len(taskList) == 0 {
		api.RespondError(c, 404, "No tasks found for this client")
		return
	}
	responseTasks := make([]gin.H, 0, len(taskList))
	for _, task := range taskList {
		responseTasks = append(responseTasks, gin.H{
			"task_id": task.TaskId,
			"clients": task.Clients,
			"command": "",
		})
	}
	api.RespondSuccess(c, responseTasks)
}

func GetSpecificTaskResult(c *gin.Context) {
	taskId := c.Param("task_id")
	clientId := c.Param("uuid")
	if taskId == "" || clientId == "" {
		api.RespondError(c, 400, "Task ID and Client ID are required")
		return
	}
	result, err := tasks.GetSpecificTaskResult(taskId, clientId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve task result: "+err.Error())
		return
	}
	if result == nil {
		api.RespondError(c, 404, "No result found for this task and client")
		return
	}
	api.RespondSuccess(c, result)
}

// Param: task_id
func GetTaskResultsByTaskId(c *gin.Context) {
	taskId := c.Param("task_id")
	if taskId == "" {
		api.RespondError(c, 400, "Task ID is required")
		return
	}
	results, err := tasks.GetTaskResultsByTaskId(taskId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve task results: "+err.Error())
		return
	}
	if len(results) == 0 {
		api.RespondError(c, 404, "No results found for this task")
		return
	}
	api.RespondSuccess(c, results)
}

func GetAllTaskResultByUUID(c *gin.Context) {
	clientId := c.Param("uuid")
	if clientId == "" {
		api.RespondError(c, 400, "Client ID is required")
		return
	}
	results, err := tasks.GetAllTasksResultByUUID(clientId)
	if err != nil {
		api.RespondError(c, 500, "Failed to retrieve tasks: "+err.Error())
		return
	}
	if len(results) == 0 {
		api.RespondError(c, 404, "No tasks found for this client")
		return
	}
	api.RespondSuccess(c, results)
}
