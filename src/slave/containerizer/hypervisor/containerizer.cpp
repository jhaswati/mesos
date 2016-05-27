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

#include <string>

#include <mesos/slave/container_logger.hpp>

#include <process/check.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/io.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>

#include <stout/hashmap.hpp>
#include <stout/os.hpp>

#include "slave/paths.hpp"
#include "slave/slave.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/hypervisor/containerizer.hpp"
#include "slave/containerizer/fetcher.hpp"


using std::map;
using std::string;
using std::vector;

using namespace process;

using mesos::slave::ContainerLogger;

namespace mesos {
namespace internal {
namespace slave {

Try<HypervisorContainerizer*> HypervisorContainerizer::create(
    const Flags& flags,
    Fetcher* fetcher)
{
  // Create and initialize the container logger module.
  Try<ContainerLogger*> logger =
    ContainerLogger::create(flags.container_logger);

  if (logger.isError()) {
    return Error("Failed to create container logger: " + logger.error());
  }

  return new HypervisorContainerizer(flags,
      fetcher,
      Owned<ContainerLogger>(logger.get()));
}


HypervisorContainerizer::HypervisorContainerizer(
    const process::Owned<HypervisorContainerizerProcess>& _process)
  :process(_process)
{
  spawn(process.get());
}


HypervisorContainerizer::HypervisorContainerizer(
    const Flags& flags,
    Fetcher* fetcher,
    const Owned<ContainerLogger>& logger)
  : process(new HypervisorContainerizerProcess(
      flags,
      fetcher,
      logger))
{
  spawn(process.get());
}


HypervisorContainerizer::~HypervisorContainerizer()
{
  terminate(process.get());
  process::wait(process.get());
}


hypervisor::Flags hypervisorFlags(
  const Flags& flags,
  const string& name,
  const string& directory)
{
  hypervisor::Flags hypervisorFlags;
  hypervisorFlags.container = name;
  hypervisorFlags.sandbox_directory = directory;
  hypervisorFlags.mapped_directory = flags.sandbox_directory;
  hypervisorFlags.stop_timeout = flags.docker_stop_timeout;
  hypervisorFlags.launcher_dir = flags.launcher_dir;
  return hypervisorFlags;
}


Try<HypervisorContainerizerProcess::Container*>
HypervisorContainerizerProcess::Container::create(
    const ContainerID& id,
    const Option<TaskInfo>& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint,
    const Flags& flags)
{
  // Before we do anything else we first make sure the stdout/stderr
  // files exist and have the right file ownership.
  Try<Nothing> touch = os::touch(path::join(directory, "stdout"));

  if (touch.isError()) {
    return Error("Failed to touch 'stdout': " + touch.error());
  }

  touch = os::touch(path::join(directory, "stderr"));

  if (touch.isError()) {
    return Error("Failed to touch 'stderr': " + touch.error());
  }

  if (user.isSome()) {
    Try<Nothing> chown = os::chown(user.get(), directory);

    if (chown.isError()) {
      return Error("Failed to chown: " + chown.error());
    }
  }

  string containerWorkdir = directory;

  Option<ContainerInfo> containerInfo = None();
  Option<CommandInfo> commandInfo = None();
  Option<map<string, string>> environment = None();

  CommandInfo newCommandInfo;
  CommandInfo::URI* diskURI = newCommandInfo.add_uris();
  diskURI->set_value("/var/lib/libvirt/images/disk.img");
  diskURI->set_executable(true);
  diskURI->set_extract(false);
  diskURI->set_cache(false);
  diskURI->set_output_file("disk.img");

  CommandInfo::URI* diskOrigURI = newCommandInfo.add_uris();
  diskOrigURI->set_value("/var/lib/libvirt/images/disk.img.orig");
  diskOrigURI->set_executable(true);
  diskOrigURI->set_extract(false);
  diskOrigURI->set_cache(false);
  diskOrigURI->set_output_file("disk.img.orig");

  commandInfo = newCommandInfo;

  return new Container(
      id,
      taskInfo,
      executorInfo,
      containerWorkdir,
      user,
      slaveId,
      slavePid,
      checkpoint,
      flags,
      commandInfo,
      containerInfo,
      environment);
}


Future<Nothing> HypervisorContainerizerProcess::fetch(
    const ContainerID& containerId,
    const SlaveID& slaveId)
{
  CHECK(containers_.contains(containerId));
  Container* container = containers_[containerId];

  return fetcher->fetch(
      containerId,
      container->command,
      container->directory,
      None(),
      slaveId,
      flags);
}


Future<Nothing> HypervisorContainerizer::recover(
  const Option<state::SlaveState>& state)
{
  return dispatch(process.get(),
                  &HypervisorContainerizerProcess::recover, state);
}

Future<bool> HypervisorContainerizer::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const std::string& directory,
    const Option<std::string>& user,
    const SlaveID& slaveId,
    const process::PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process.get(),
           &HypervisorContainerizerProcess::launch,
           containerId,
           None(),
           executorInfo,
           directory,
           user,
           slaveId,
           slavePid,
           checkpoint);
}

Future<bool> HypervisorContainerizer::launch(
  const ContainerID& containerId,
  const TaskInfo& taskInfo,
  const ExecutorInfo& executorInfo,
  const std::string& directory,
  const Option<std::string>& user,
  const SlaveID& slaveId,
  const process::PID<Slave>& slavePid,
  bool checkpoint)
{
    return dispatch(process.get(),
                    &HypervisorContainerizerProcess::launch,
                    containerId,
                    taskInfo,
                    executorInfo,
                    directory,
                    user,
                    slaveId,
                    slavePid,
                    checkpoint);
}

Future<Nothing> HypervisorContainerizer::update(
  const ContainerID& containerId,
  const Resources& resources)
{
  return dispatch(
      process.get(),
      &HypervisorContainerizerProcess::update,
      containerId,
      resources,
      false);
}

Future<ResourceStatistics> HypervisorContainerizer::usage(
  const ContainerID& containerId)
{
  return Failure("This functionality is not supported");
}

/*Future<ContainerStatus> HypervisorContainerizer::status(
  const ContainerID& containerId)
{
  return Failure("This functionality is not supported");
}*/

Future<containerizer::Termination> HypervisorContainerizer::wait(
  const ContainerID& containerId)
{
  return dispatch(
      process.get(),
      &HypervisorContainerizerProcess::wait,
      containerId);
}

void HypervisorContainerizer::destroy(const ContainerID& containerId)
{
}

Future<hashset<ContainerID>> HypervisorContainerizer::containers()
{
  return dispatch(process.get(), &HypervisorContainerizerProcess::containers);
}

Future<Nothing> HypervisorContainerizerProcess::recover(
                  const Option<state::SlaveState>& state)
{
  return Nothing();
}

Future<bool> HypervisorContainerizerProcess::launch(
  const ContainerID& containerId,
  const Option<TaskInfo>& taskInfo,
  const ExecutorInfo& _executorInfo,
  const string& directory,
  const Option<string>& user,
  const SlaveID& slaveId,
  const PID<Slave>& slavePid,
  bool checkpoint)
{
  if (containers_.contains(containerId)) {
    return Failure("Container already started");
  }

  /*if (taskInfo.isSome() &&
      taskInfo.get().has_container() &&
      taskInfo.get().container().type() != ContainerInfo::HYPERVISOR) {
    return false;
  }*/

  // NOTE: We make a copy of the executor info because we may mutate
  // it with default container info.
  ExecutorInfo executorInfo = _executorInfo;

  /*if (executorInfo.has_container() &&
    executorInfo.container().type() != ContainerInfo::HYPERVISOR) {
    return false;
  }*/

  // Add the default container info to the executor info.
  // TODO(jieyu): Rename the flag to be default_mesos_container_info.
  if (!executorInfo.has_container() &&
      flags.default_container_info.isSome()) {
    executorInfo.mutable_container()->CopyFrom(
        flags.default_container_info.get());
  }

  LOG(INFO) << "Starting container '" << containerId
            << "' for executor '" << executorInfo.executor_id()
            << "' of framework '" << executorInfo.framework_id() << "'";

  LOG(INFO) << "Sandbox directory is" << directory;

  Try<Container*> container = Container::create(
      containerId,
      taskInfo,
      executorInfo,
      directory,
      user,
      slaveId,
      slavePid,
      checkpoint,
      flags);

  if (container.isError()) {
    return Failure("Failed to create container: " + container.error());
  }

  containers_[containerId] = container.get();

  if (taskInfo.isSome()) {
    LOG(INFO) << "Starting container '" << containerId
              << "' for task '" << taskInfo.get().task_id()
              << "' (and executor '" << executorInfo.executor_id()
              << "') of framework '" << executorInfo.framework_id() << "'";
  } else {
    LOG(INFO) << "Starting container '" << containerId
              << "' for executor '" << executorInfo.executor_id()
              << "' and framework '" << executorInfo.framework_id() << "'";
  }

  string containerName = container.get()->name();

  if (container.get()->executorName().isSome()) {
    // Launch the container with the executor name as we expect the
    // executor will launch the hypervisor container.
    containerName = container.get()->executorName().get();
  }

  return container.get()->launch = fetch(containerId, slaveId)
    .then(defer(self(), [=]() {
    return launchExecutorProcess(containerId);
    }))
    .then(defer(self(), [=](pid_t pid) {
      return reapExecutor(containerId, pid);
    }));
}

Future<pid_t> HypervisorContainerizerProcess::launchExecutorProcess(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Container is already destroyed");
  }

  Container* container = containers_[containerId];
  container->state = Container::RUNNING;

  // Prepare environment variables for the executor.
  map<string, string> environment = executorEnvironment(
      container->executor,
      container->directory,
      container->slaveId,
      container->slavePid,
      false,
      flags,
      false);

  // Include any enviroment variables from ExecutorInfo.
  foreach (const Environment::Variable& variable,
           container->executor.command().environment().variables()) {
    environment[variable.name()] = variable.value();
  }

  // Pass GLOG flag to the executor.
  const Option<string> glog = os::getenv("GLOG_v");
  if (glog.isSome()) {
    environment["GLOG_v"] = glog.get();
  }
  vector<string> argv;
  argv.push_back("mesos-hypervisor-executor");

  std::vector<Subprocess::Hook> parentHooks;

  return logger->prepare(container->executor, container->directory)
    .then(defer(
        self(),
        [=](const ContainerLogger::SubprocessInfo& subprocessInfo)
          -> Future<pid_t> {
  Try<Subprocess> s = subprocess(
      path::join(flags.launcher_dir, "mesos-hypervisor-executor"),
      argv,
      Subprocess::PIPE(),
      // subprocessInfo.out,
      // subprocessInfo.err,
      Subprocess::FD(STDOUT_FILENO),
      Subprocess::FD(STDERR_FILENO),
      Setsid::SETSID,
      hypervisorFlags(flags, container->name(), container->directory),
      environment,
      None(),
      parentHooks,
      container->directory);

    if (s.isError()) {
      return Failure("Failed to fork executor: " + s.error());
    }
    return s.get().pid();
    }));
}

Future<hashset<ContainerID>> HypervisorContainerizerProcess::containers()
{
  return containers_.keys();
}
/*Future<bool> HypervisorContainerizerProcess::_launch()
{
  virConnectPtr conn;
  conn = virConnectOpen("qemu:///system");
  if (conn == NULL) {
    fprintf(stderr, "Failed to open connection to qemu:///system\n");
    return 1;
   }
  virConnectClose(conn);

  return true;
}*/


Future<containerizer::Termination> HypervisorContainerizerProcess::wait(
    const ContainerID& containerId)
{
  if (!containers_.contains(containerId)) {
    return Failure("Unknown container: " + stringify(containerId));
  }

  return containers_[containerId]->termination.future();
}


Future<Nothing> HypervisorContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& _resources,
    bool force)
{
  return Nothing();
}


Future<bool> HypervisorContainerizerProcess::reapExecutor(
    const ContainerID& containerId,
    pid_t pid)
{
  // After we do Docker::run we shouldn't remove a container until
  // after we set 'status', which we do in this function.
  // CHECK(containers_.contains(containerId));

  // Container* container = containers_[containerId];

  // And finally watch for when the container gets reaped.
  // container->status.set(process::reap(pid));

  // container->status.future().get()
    // .onAny(defer(self(), &Self::reaped, containerId));

  return true;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
