package org.idark.spark.ssh

import java.io.{FileInputStream, InputStream, File}

import com.jcraft.jsch.{ChannelExec, JSch, Session}

/**
 * Executes spark job on the remote cluster.
 */
object SSHRemoteSparkExecutor {

  def run(uberJarFile: File, username: String, host: String, localPath: String, sparkSubmitPath: String, sparkBin: String) = {
    val dest = SSHTarget(username, host, localPath)

    val jsch = new JSch()
    val session = jsch.getSession(dest.userName, dest.hostName, 22)

    copy(uberJarFile, dest, session)
    start(dest, sparkSubmitPath, session)

    session.disconnect()
  }

  private def start(dest: SSHTarget, sparkSubmitPath: String, session: Session): Unit = {
    val channel = session.openChannel("exec")
    val sparkSubmitJob = sparkSubmitPath + " " + dest.localPath
    channel.asInstanceOf[ChannelExec].setCommand(sparkSubmitJob)
    channel.setInputStream(null)

    channel.asInstanceOf[ChannelExec].setErrStream(System.err)

    val in = channel.getInputStream

    channel.connect()

    val buffer: Array[Byte] = new Array[Byte](1024)
    var count: Int = 0

    while (true) {
      while ( {
        count = in.read(buffer, 0, buffer.length)
        count > 0
      }) {
        System.out.print(new String(buffer, 0, count))
      }
      if (channel.isClosed) {
        System.out.println(channel.getExitStatus)
      }
    }
    channel.disconnect()
  }

  private def copy(uberJarFile: File, dest: SSHTarget, session: Session): Unit = {
    val channel = session.openChannel("exec")
    // get I/O streams for remote scp
    val out = channel.getOutputStream
    val in = channel.getInputStream

    {
      val command = "scp " + " -t " + dest.localPath
      channel.asInstanceOf[ChannelExec].setCommand(command)

      channel.connect()

      if (checkAck(in) != 0) {
        System.exit(0)
      }
    }

    {
      val _lfile = uberJarFile
      // send "C0644 filesize filename", where filename should not include '/'
      val filesize = _lfile.length()
      val command = "C0644 " + filesize + " " + uberJarFile.getName + "\n"
      out.write(command.getBytes)
      out.flush()
      if (checkAck(in) != 0) {
        System.exit(0)
      }
    }

    //TODO managed resource should be here
    // send a content of lfile
    val fis = new FileInputStream(uberJarFile)
    val buf: Array[Byte] = new Array[Byte](1024)

    val buffer = Array.ofDim[Byte](1024)
    var count = -1

    while ( {
      count = fis.read(buffer, 0, buffer.length)
      count > 0
    }) {
      out.write(buffer, 0, count)
    }

    fis.close()



    // send '\0'
    buf(0) = 0
    out.write(buf, 0, 1)
    out.flush()
    if (checkAck(in) != 0) {
      System.exit(0)
    }
    out.close()

    channel.disconnect()
  }

  def checkAck(in: InputStream): Int = {
    val b = in.read()
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if (b == 0) return b
    if (b == -1) return b

    if (b == 1 || b == 2) {
      val sb = new StringBuffer()
      var c: Int = 0
      do {
        c = in.read()
        sb.append(c)
      }
      while (c != '\n')
      if (b == 1) {
        // error
        System.out.print(sb.toString())
      }
      if (b == 2) {
        // fatal error
        System.out.print(sb.toString())
      }
    }
    b
  }

  private def parse(host: String) = {
    val credentialsAndTarget: Array[String] = host.split("@")
    val hostNameAndPath: Array[String] = credentialsAndTarget(1).split(":")
    SSHTarget(credentialsAndTarget(0), hostNameAndPath(0), hostNameAndPath(1))
  }

  private case class SSHTarget(userName: String, hostName: String, localPath: String)

}
