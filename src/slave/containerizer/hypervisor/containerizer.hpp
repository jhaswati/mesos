// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __HYPERVISOR_CONTAINERIZER_HPP__
#define __HYPERVISOR_CONTAINERIZER_HPP__

#include <mesos/slave/container_logger.hpp>

#include <stout/flags.hpp>
#include <stout/hashset.hpp>

#include "hypervisor/executor.hpp"

#include <list>
#include <vector>

#include "slave/containerizer/containerizer.hpp"

namespace mesos {
namespace internal {
namespace slave {

// Forward declaration.
class HypervisorContainerizerProcess;

class HypervisorContainerizer : public Containerizer
{
public:
  static Try<HypervisorContainerizer*> create(
      const Flags& flags,
      Fetcher* fetcher);

  HypervisorContainerizer(
      const Flags& flags,
      Fetcher* fetcher,
      const process::Owned<mesos::slave::ContainerLogger>& logger);

  // Used for testing.
  HypervisorContainerizer(
      const process::Owned<HypervisorContainerizerProcess>& _process);

  virtual ~HypervisorContainerizer();

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

/*  virtual process::Future<ContainerStatus> status(
      const ContainerID& containerId);*/

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();


private:
  process::Owned<HypervisorContainerizerProcess> process;
};

class HypervisorContainerizerProcess
  : public process::Process<HypervisorContainerizerProcess>
{
public:
  HypervisorContainerizerProcess(
      const Flags& _flags,
      Fetcher* _fetcher,
      const process::Owned<mesos::slave::ContainerLogger>& _logger)
    : flags(_flags),
      fetcher(_fetcher),
      logger(_logger) {}

  virtual ~HypervisorContainerizerProcess() {}

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future<bool> launch(
      const ContainerID& containerId,
      const Option<TaskInfo>& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  // force = true causes the containerizer to update the resources
  // for the container, even if they match what it has cached.
  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources,
      bool force);

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual process::Future<Nothing> fetch(
      const ContainerID& containerId,
      const SlaveID& slaveId);

  // Reaps on the executor pid.
  process::Future<bool> reapExecutor(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<hashset<ContainerID>> containers();

private:
  // Starts the hypervisor executor with a subprocess.
  process::Future<pid_t> launchExecutorProcess(
      const ContainerID& containerId);

  const Flags flags;

  Fetcher* fetcher;

  process::Owned<mesos::slave::ContainerLogger> logger;

  struct Container
  {
    static Try<Container*> create(
        const ContainerID& id,
        const Option<TaskInfo>& taskInfo,
        const ExecutorInfo& executorInfo,
        const std::string& directory,
        const Option<std::string>& user,
        const SlaveID& slaveId,
        const process::PID<Slave>& slavePid,
        bool checkpoint,
        const Flags& flags);

    static std::string name(const SlaveID& slaveId, const std::string& id)
    {
      return "Hypervisor" + slaveId.value() + stringify(id);
    }

    Container(const ContainerID& id)
      : state(FETCHING), id(id) {}

    Container(const ContainerID& id,
              const Option<TaskInfo>& taskInfo,
              const ExecutorInfo& executorInfo,
              const std::string& directory,
              const Option<std::string>& user,
              const SlaveID& slaveId,
              const process::PID<Slave>& slavePid,
              bool checkpoint,
              const Flags& flags,
              const Option<CommandInfo>& _command,
              const Option<ContainerInfo>& _container,
              const Option<std::map<std::string, std::string>>& _environment)
      : state(FETCHING),
        id(id),
        task(taskInfo),
        executor(executorInfo),
        directory(directory),
        user(user),
        slaveId(slaveId),
        slavePid(slavePid),
        checkpoint(checkpoint),
        flags(flags)
    {
      // NOTE: The task's resources are included in the executor's
      // resources in order to make sure when launching the executor
      // that it has non-zero resources in the event the executor was
      // not actually given any resources by the framework
      // originally. See Framework::launchExecutor in slave.cpp. We
      // check that this is indeed the case here to protect ourselves
      // from when/if this changes in the future (but it's not a
      // perfect check because an executor might always have a subset
      // of it's resources that match a task, nevertheless, it's
      // better than nothing).
      resources = executor.resources();

      if (task.isSome()) {
        CHECK(resources.contains(task.get().resources()));
      }

      if (_command.isSome()) {
        command = _command.get();
      } else if (task.isSome()) {
        command = task.get().command();
      } else {
        command = executor.command();
      }

      if (_container.isSome()) {
        container = _container.get();
      } else if (task.isSome()) {
        container = task.get().container();
      } else {
        container = executor.container();
      }

      if (_environment.isSome()) {
        environment = _environment.get();
      } else {
        environment = executorEnvironment(
            executor,
            directory,
            slaveId,
            slavePid,
            false,
            flags,
            false);
      }
    }

    ~Container()
    {
    }

    std::string name()
    {
      return name(slaveId, stringify(id));
    }

    Option<std::string> executorName()
    {
        return None();
    }

    /*std::string image() const
    {
      if (task.isSome()) {
        return task.get().container().docker().image();
      }

      return executor.container().docker().image();
    }*/


    // The DockerContainerier needs to be able to properly clean up
    // Docker containers, regardless of when they are destroyed. For
    // example, if a container gets destroyed while we are fetching,
    // we need to not keep running the fetch, nor should we try and
    // start the Docker container. For this reason, we've split out
    // the states into:
    //
    //     FETCHING
    //     PULLING
    //     MOUNTING
    //     RUNNING
    //     DESTROYING
    //
    // In particular, we made 'PULLING' be it's own state so that we
    // could easily destroy and cleanup when a user initiated pulling
    // a really big image but we timeout due to the executor
    // registration timeout. Since we curently have no way to discard
    // a Docker::run, we needed to explicitely do the pull (which is
    // the part that takes the longest) so that we can also explicitly
    // kill it when asked. Once the functions at Docker::* get support
    // for discarding, then we won't need to make pull be it's own
    // state anymore, although it doesn't hurt since it gives us
    // better error messages.
    enum State
    {
      FETCHING = 1,
      RUNNING = 2,
      DESTROYING = 3
    } state;

    const ContainerID id;
    const Option<TaskInfo> task;
    const ExecutorInfo executor;
    ContainerInfo container;
    CommandInfo command;
    std::map<std::string, std::string> environment;

    // The sandbox directory for the container. This holds the
    // symlinked path if symlinked boolean is true.
    std::string directory;

    const Option<std::string> user;
    SlaveID slaveId;
    const process::PID<Slave> slavePid;
    bool checkpoint;
    const Flags flags;

    // Promise for future returned from wait().
    process::Promise<containerizer::Termination> termination;

    // Exit status of executor or container (depending on whether or
    // not we used the command executor). Represented as a promise so
    // that destroying can chain with it being set.
    // process::Promise<process::Future<Option<int>>> status;

    // Future that tells us the return value of last launch stage (fetch, pull,
    // run, etc).
    process::Future<bool> launch;

    // We keep track of the resources for each container so we can set
    // the ResourceStatistics limits in usage(). Note that this is
    // different than just what we might get from TaskInfo::resources
    // or ExecutorInfo::resources because they can change dynamically.
    Resources resources;

    // Once the container is running, this saves the pid of the
    // running container.
    Option<pid_t> pid;

    // The executor pid that was forked to wait on the running
    // container. This is stored so we can clean up the executor
    // on destroy.
    Option<pid_t> executorPid;
  };

  hashmap<ContainerID, Container*> containers_;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {
#endif
