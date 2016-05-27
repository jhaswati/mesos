// Licensed to the Apache Software Foundation (ASF) under one

#include <stdio.h>

#include <string>

#include <libvirt/libvirt.h>

#include <mesos/mesos.hpp>
#include <mesos/executor.hpp>

#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/subprocess.hpp>
#include <process/reap.hpp>
#include <process/owned.hpp>

#include <stout/flags.hpp>
#include <stout/protobuf.hpp>
#include <stout/os.hpp>
#include <stout/strings.hpp>

#include <stout/os/getcwd.hpp>
#include <stout/os/write.hpp>

#include "common/status_utils.hpp"

#include "hypervisor/executor.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "messages/messages.hpp"

using namespace mesos;
using namespace process;

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

using process::subprocess;
using process::Subprocess;

namespace mesos {
namespace internal {
namespace hypervisor {

// const Duration DOCKER_INSPECT_DELAY = Milliseconds(500);
// const Duration DOCKER_INSPECT_TIMEOUT = Seconds(5);

// Executor that is responsible to execute a docker container and
// redirect log output to configured stdout and stderr files. Similar
// to `CommandExecutor`, it launches a single task (a docker container)
// and exits after the task finishes or is killed. The executor assumes
// that it is launched from the `DockerContainerizer`.
class HypervisorExecutorProcess
  : public ProtobufProcess<HypervisorExecutorProcess>
{
public:
  HypervisorExecutorProcess(
      const string& container,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const string& healthCheckDir)
    : killed(false),
      killedByHealthCheck(false),
      healthPid(-1),
      container(container),
      healthCheckDir(healthCheckDir),
      sandboxDirectory(sandboxDirectory),
      mappedDirectory(mappedDirectory) {}

  virtual ~HypervisorExecutorProcess() {}

  void registered(
      ExecutorDriver* _driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& _frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    cout << "Registered docker executor on " << slaveInfo.hostname() << endl;
    driver = _driver;
    frameworkInfo = _frameworkInfo;
  }

  void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered docker executor on " << slaveInfo.hostname() << endl;
  }

  void disconnected(ExecutorDriver* driver)
  {
    cout << "Disconnected from the slave" << endl;
  }

  void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    /*if (run.isSome()) {
      TaskStatus status;
      status.mutable_task_id()->CopyFrom(task.task_id());
      status.set_state(TASK_FAILED);
      status.set_message(
          "Attempted to run multiple tasks using a \"docker\" executor");

      driver->sendStatusUpdate(status);
      return;
    }*/

    // Capture the TaskID.
    taskId = task.task_id();

    // Capture the command.
    command = task.command().value();

    // cout << "Starting task " << taskId.get() << endl;
    LOG(INFO) << "Here I am";

    /*CHECK(task.has_container());
    CHECK(task.has_command());

    CHECK(task.container().type() == ContainerInfo::DOCKER);*/

      // Before we do anything else we first make sure the stdout/stderr
      // files exist and have the right file ownership.
      string directory = os::getcwd();
      Try<Nothing> touch = os::touch(path::join(directory, "meta-data"));

      if (touch.isError()) {
        LOG(ERROR) << "Failed to touch 'meta-data': " << touch.error();
        return;
      }

      touch = os::touch(path::join(directory, "user-data"));

      if (touch.isError()) {
        LOG(ERROR) << "Failed to touch 'user-data': " << touch.error();
        return;
      }

      string userData = R"bar(#cloud-config
password: passw0rd
chpasswd: { expire: False }
ssh_pwauth: True
runcmd:
 - [ sh, -c, "@command@" ]
        )bar";
      userData = strings::replace(userData, "@command@", command.get());
      Try<Nothing> write = os::write(path::join(directory, "user-data"),
                                     userData);
      if (write.isError()) {
        LOG(ERROR) << "Failed to write 'user-data': " << write.error();
        return;
      }

    Try<Subprocess> seedProcess = subprocess(
      "cloud-localds seed.img user-data",
      Subprocess::FD(STDIN_FILENO),
      Subprocess::PATH(path::join(directory, "stdout")),
      Subprocess::PATH(path::join(directory, "stderr")));

    if (seedProcess.isError()) {
      LOG(ERROR) << "Failed to start 'cloud-localds': " << seedProcess.error();
      return;
    }

/*      Try<Nothing> rm = os::rm(path::join(directory, "user-data"));

      if (rm.isError()) {
        LOG(ERROR) << "Failed to remove 'user-data': " << rm.error();
        return;
      }*/

    virConnectPtr conn;
      conn = virConnectOpen("qemu:///system");
      if (conn == NULL) {
        cout << "Failed to open connection to qemu:///system" << endl;
        return;
       }

    const char* s1 = R"foo(
      <domain type='kvm'>
        <name>@vmname@</name>
        <memory>524288</memory>
        <os>
          <type arch='x86_64' machine='pc-i440fx-utopic'>hvm</type>
          <boot dev='hd'/>
        </os>
       <features>
       <acpi/><apic/><pae/>
       </features>
       <clock offset="utc"/>
       <on_poweroff>destroy</on_poweroff>
        <on_reboot>restart</on_reboot>
        <on_crash>restart</on_crash>
        <vcpu>1</vcpu>
        <devices>
           <emulator>/usr/bin/kvm-spice</emulator>
           <disk type='file' device='disk'>
           <driver name='qemu' type='qcow2'/>
           <source file='@diskimage@'/>
           <backingStore type='file' index='1'>
             <format type='raw'/>
             <source file='@diskimageoriginal@'/>
           <backingStore/>
           </backingStore>
           <target dev='hda' bus='ide'/>
           </disk>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='@seedimage@'/>
      <backingStore/>
      <target dev='hdb' bus='ide'/>
      <readonly/>
    </disk>
    <interface type='bridge'>
      <source bridge='virbr0'/>
      <target dev='vnet0'/>
      <model type='rtl8139'/>
    </interface>
    <channel type='unix'>
      <source mode='bind' path='/var/lib/libvirt/qemu/f16x86_64.agent'/>
      <target type='virtio' name='org.qemu.guest_agent.0'/>
    </channel>
    <graphics type='vnc' port='-1'/>
    <console type='pty'/>
  </devices>
      </domain>
    )foo";

    string domainXML = s1;

    domainXML = strings::replace(
                  domainXML,
                  "@vmname@",
                  container.substr(container.length()-12));
    domainXML = strings::replace(
                  domainXML,
                  "@diskimage@",
                  directory+"/disk.img");
    domainXML = strings::replace(
                  domainXML,
                  "@diskimageoriginal@",
                  directory+"/disk.img.orig");
    domainXML = strings::replace(
                  domainXML,
                  "@seedimage@",
                  directory+"/seed.img");

    cout << domainXML << endl;
    cout << os::getcwd() << endl;

    s1 = domainXML.c_str();

    virDomainPtr domainPtr = virDomainDefineXML(conn, s1);

    int success = virDomainCreate(domainPtr);

    if(success == 1) {
      virConnectClose(conn);
      return;
    }

    virConnectClose(conn);


    dispatch(self(), &Self::reaped, driver);

    // Delay sending TASK_RUNNING status update until we receive
    // inspect output.
    /*inspect = docker->inspect(containerName, DOCKER_INSPECT_DELAY)
      .then(defer(self(), [=](const Docker::Container& container) {
        if (!killed) {
          TaskStatus status;
          status.mutable_task_id()->CopyFrom(taskId.get());
          status.set_state(TASK_RUNNING);
          status.set_data(container.output);
          if (container.ipAddress.isSome()) {
            // TODO(karya): Deprecated -- Remove after 0.25.0 has shipped.
            Label* label = status.mutable_labels()->add_labels();
            label->set_key("Docker.NetworkSettings.IPAddress");
            label->set_value(container.ipAddress.get());

            NetworkInfo* networkInfo =
              status.mutable_container_status()->add_network_infos();

            // TODO(CD): Deprecated -- Remove after 0.27.0.
            networkInfo->set_ip_address(container.ipAddress.get());

            NetworkInfo::IPAddress* ipAddress =
              networkInfo->add_ip_addresses();
            ipAddress->set_ip_address(container.ipAddress.get());
          }
          driver->sendStatusUpdate(status);
        }

        return Nothing();
      }));*/

    // inspect.onReady(
        // defer(self(), &Self::launchHealthCheck, containerName, task));
  }

  void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    cout << "Received killTask" << endl;

    // Since the docker executor manages a single task, we
    // shutdown completely when we receive a killTask.
    shutdown(driver);
  }

  void frameworkMessage(ExecutorDriver* driver, const string& data) {}

  void shutdown(ExecutorDriver* driver)
  {
    cout << "Shutting down" << endl;

    if (!killed) {
      // Send TASK_KILLING if the framework can handle it.
      // CHECK_SOME(frameworkInfo);
      // CHECK_SOME(taskId);

      foreach (const FrameworkInfo::Capability& c,
               frameworkInfo->capabilities()) {
        if (c.type() == FrameworkInfo::Capability::TASK_KILLING_STATE) {
          TaskStatus status;
          status.mutable_task_id()->CopyFrom(taskId.get());
          status.set_state(TASK_KILLING);
          driver->sendStatusUpdate(status);
          break;
        }
      }

      killed = true;
    }

    // Cleanup health check process.
    //
    // TODO(bmahler): Consider doing this after the task has been
    // reaped, since a framework may be interested in health
    // information while the task is being killed (consider a
    // task that takes 30 minutes to be cleanly killed).
    if (healthPid != -1) {
      os::killtree(healthPid, SIGKILL);
    }
  }

  void error(ExecutorDriver* driver, const string& message) {}

protected:
  virtual void initialize()
  {
    install<TaskHealthStatus>(
        &Self::taskHealthUpdated,
        &TaskHealthStatus::task_id,
        &TaskHealthStatus::healthy,
        &TaskHealthStatus::kill_task);
  }

  void taskHealthUpdated(
      const TaskID& taskID,
      const bool& healthy,
      const bool& initiateTaskKill)
  {
    if (driver.isNone()) {
      return;
    }

    cout << "Received task health update, healthy: "
         << stringify(healthy) << endl;

    TaskStatus status;
    status.mutable_task_id()->CopyFrom(taskID);
    status.set_healthy(healthy);
    status.set_state(TASK_RUNNING);
    driver.get()->sendStatusUpdate(status);

    if (initiateTaskKill) {
      killedByHealthCheck = true;
      killTask(driver.get(), taskID);
    }
  }

private:
  void reaped(
      ExecutorDriver* _driver)
  {
    // Wait for docker->stop to finish, and best effort wait for the
    // inspect future to complete with a timeout.
    /*stop.onAny(defer(self(), [=](const Future<Nothing>&) {
      inspect
        .after(DOCKER_INSPECT_TIMEOUT, [=](const Future<Nothing>&) {
          inspect.discard();
          return inspect;
        })
        .onAny(defer(self(), [=](const Future<Nothing>&) {
          CHECK_SOME(driver);
          TaskState state;
          string message;
          if (!stop.isReady()) {
            state = TASK_FAILED;
            message = "Unable to stop docker container, error: " +
                      (stop.isFailed() ? stop.failure() : "future discarded");
          } else if (killed) {
            state = TASK_KILLED;
          } else if (!run.isReady()) {
            state = TASK_FAILED;
            message = "Docker container run error: " +
                      (run.isFailed() ?
                       run.failure() : "future discarded");
          } else {
            state = TASK_FINISHED;
          }

          CHECK_SOME(taskId);

          TaskStatus taskStatus;
          taskStatus.mutable_task_id()->CopyFrom(taskId.get());
          taskStatus.set_state(state);
          taskStatus.set_message(message);
          if (killed && killedByHealthCheck) {
            taskStatus.set_healthy(false);
          }

          driver.get()->sendStatusUpdate(taskStatus);

          // A hack for now ... but we need to wait until the status update
          // is sent to the slave before we shut ourselves down.
          // TODO(tnachen): Remove this hack and also the same hack in the
          // command executor when we have the new HTTP APIs to wait until
          // an ack.
          os::sleep(Seconds(1));
          driver.get()->stop();
        }));
    }));*/
    // CHECK_SOME(driver);
    TaskState state;
    string message;

    if (killed) {
      state = TASK_KILLED;
    }

    // CHECK_SOME(taskId);

    TaskStatus taskStatus;
    taskStatus.mutable_task_id()->CopyFrom(taskId.get());
    taskStatus.set_state(state);
    taskStatus.set_message(message);
    if (killed && killedByHealthCheck) {
      taskStatus.set_healthy(false);
    }

    driver.get()->sendStatusUpdate(taskStatus);

    // A hack for now ... but we need to wait until the status update
    // is sent to the slave before we shut ourselves down.
    // TODO(tnachen): Remove this hack and also the same hack in the
    // command executor when we have the new HTTP APIs to wait until
    // an ack.
    os::sleep(Seconds(1));
    driver.get()->stop();
  }

  void launchHealthCheck(const string& containerName, const TaskInfo& task)
  {
    if (!killed && task.has_health_check()) {
      HealthCheck healthCheck = task.health_check();

      // Wrap the original health check command in "docker exec".
      /*if (healthCheck.has_command()) {
          CommandInfo command = healthCheck.command();

          vector<string> argv;
          argv.push_back(docker->getPath());
          argv.push_back("exec");
          argv.push_back(containerName);

          if (command.shell()) {
            if (!command.has_value()) {
              cerr << "Unable to launch health process: "
                   << "Shell command is not specified." << endl;
              return;
            }

            argv.push_back("sh");
            argv.push_back("-c");
            argv.push_back("\"");
            argv.push_back(command.value());
            argv.push_back("\"");
          } else {
            if (!command.has_value()) {
              cerr << "Unable to launch health process: "
                   << "Executable path is not specified." << endl;
              return;
            }

            argv.push_back(command.value());
            foreach (const string& argument, command.arguments()) {
              argv.push_back(argument);
            }
          }

          command.set_shell(true);
          command.clear_arguments();
          command.set_value(strings::join(" ", argv));
          healthCheck.mutable_command()->CopyFrom(command);
      } else {
          cerr << "Unable to launch health process: "
               << "Only command health check is supported now." << endl;
          return;
      }*/

      JSON::Object json = JSON::protobuf(healthCheck);

      // Launch the subprocess using 'exec' style so that quotes can
      // be properly handled.
      vector<string> argv;
      string path = path::join(healthCheckDir, "mesos-health-check");
      argv.push_back(path);
      argv.push_back("--executor=" + stringify(self()));
      argv.push_back("--health_check_json=" + stringify(json));
      argv.push_back("--task_id=" + task.task_id().value());

      string cmd = strings::join(" ", argv);
      cout << "Launching health check process: " << cmd << endl;

      Try<Subprocess> healthProcess =
        process::subprocess(
          path,
          argv,
          // Intentionally not sending STDIN to avoid health check
          // commands that expect STDIN input to block.
          Subprocess::PATH("/dev/null"),
          Subprocess::FD(STDOUT_FILENO),
          Subprocess::FD(STDERR_FILENO));

      if (healthProcess.isError()) {
        cerr << "Unable to launch health process: "
             << healthProcess.error() << endl;
      } else {
        healthPid = healthProcess.get().pid();

        cout << "Health check process launched at pid: "
             << stringify(healthPid) << endl;
      }
    }
  }

  bool killed;
  bool killedByHealthCheck;
  pid_t healthPid;
  string container;
  string healthCheckDir;
  string sandboxDirectory;
  string mappedDirectory;
  Option<ExecutorDriver*> driver;
  Option<FrameworkInfo> frameworkInfo;
  Option<TaskID> taskId;
  Option<string> command;
};


class HypervisorExecutor : public Executor
{
public:
  HypervisorExecutor(
      const string& container,
      const string& sandboxDirectory,
      const string& mappedDirectory,
      const string& healthCheckDir)
  {
    process = Owned<HypervisorExecutorProcess>(new HypervisorExecutorProcess(
        container,
        sandboxDirectory,
        mappedDirectory,
        healthCheckDir));

    spawn(process.get());
  }

  virtual ~HypervisorExecutor()
  {
    terminate(process.get());
    wait(process.get());
  }

  virtual void registered(
      ExecutorDriver* driver,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process.get(),
             &HypervisorExecutorProcess::registered,
             driver,
             executorInfo,
             frameworkInfo,
             slaveInfo);
  }

  virtual void reregistered(
      ExecutorDriver* driver,
      const SlaveInfo& slaveInfo)
  {
    dispatch(process.get(),
             &HypervisorExecutorProcess::reregistered,
             driver,
             slaveInfo);
  }

  virtual void disconnected(ExecutorDriver* driver)
  {
    dispatch(process.get(), &HypervisorExecutorProcess::disconnected, driver);
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    dispatch(process.get(),
             &HypervisorExecutorProcess::launchTask, driver, task);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId)
  {
    dispatch(process.get(),
             &HypervisorExecutorProcess::killTask, driver, taskId);
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data)
  {
    dispatch(process.get(),
             &HypervisorExecutorProcess::frameworkMessage,
             driver,
             data);
  }

  virtual void shutdown(ExecutorDriver* driver)
  {
    dispatch(process.get(), &HypervisorExecutorProcess::shutdown, driver);
  }

  virtual void error(ExecutorDriver* driver, const string& data)
  {
    dispatch(process.get(), &HypervisorExecutorProcess::error, driver, data);
  }

private:
  Owned<HypervisorExecutorProcess> process;
};


} // namespace hypervisor {
} // namespace internal {
} // namespace mesos {


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  mesos::internal::hypervisor::Flags flags;

  // Load flags from environment and command line.
  Try<flags::Warnings> load = flags.load(None(), &argc, &argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  std::cout << stringify(flags) << std::endl;

  mesos::internal::logging::initialize(argv[0], flags, true); // Catch signals.

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  std::cout << stringify(flags) << std::endl;

  mesos::internal::hypervisor::HypervisorExecutor executor(
      flags.container.get(),
      flags.sandbox_directory.get(),
      flags.mapped_directory.get(),
      flags.launcher_dir.get());

  std::cout << "Came here also";
  mesos::MesosExecutorDriver driver(&executor);
  return driver.run() == mesos::DRIVER_STOPPED ? EXIT_SUCCESS : EXIT_FAILURE;
}
