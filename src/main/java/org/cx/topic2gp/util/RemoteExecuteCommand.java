package org.cx.topic2gp.util;

import ch.ethz.ssh2.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 远程服务器执行shell脚本
 *
 */
public class RemoteExecuteCommand {
	
	private Connection conn;
	private SCPClient client;
	private String userName;
	private String password;
	private String ip;
	private final String CHARSET = "utf-8";
	private final int TIME_OUT = 1000 * 60 * 1;
	
	public RemoteExecuteCommand(String ip, String userName, String password) {
		this.ip = ip;
		this.userName = userName;
		this.password = password;
	}
	
	@SuppressWarnings("unused")
	private RemoteExecuteCommand() {
	}
	
	/**
	 * 登陆
	 * @return 登陆成功true, 否则false
	 * @
	 */
	public boolean login()  {
		boolean result = false;
		try {
			conn = new Connection(ip);
			conn.connect();
			result = conn.authenticateWithPassword(userName, password);
			if (client == null) {
				client=conn.createSCPClient();
			}
		} catch (IOException e) {
		}
		return result;
	}
	
	public int execute(String command)  {
		if (!login()) {
			LOGGER.info("远程服务器用户名或密码不正确!");
		}

		InputStream stdErr = null;
		String stdErrString = null;
		int ret = 0;
		try {
			Session session = conn.openSession();
			session.execCommand(command);
			
			stdErr = new StreamGobbler(session.getStderr());
			stdErrString = processStream(stdErr, CHARSET);
			if (StringUtils.isNotEmpty(stdErrString)) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("命令执行错误信息:" + stdErrString);
				}
			}
			
			session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
			ret = session.getExitStatus();
			if (StringUtils.isNotBlank(stdErrString)) {
				ret = -1;
			}
		} catch (Exception e) {
		} finally {
			if (null != conn) {
				conn.close();
			}
		}
		return ret;
	}
	
	public List<String> executeReturn(String command)  {
		if (!login()) {
			LOGGER.info("远程服务器用户名或密码不正确!");
		}
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info("执行命令：" + command);
		}
		List<String> lineList = new ArrayList<String>();
		String line = null;
		InputStream stdErr = null;
		String stdErrString = null;
		BufferedReader reader = null;
		
		try {
			Session session = conn.openSession();
			session.execCommand(command);
			
			reader = new BufferedReader(new InputStreamReader(new StreamGobbler(session.getStdout()), CHARSET));
			
			stdErr = new StreamGobbler(session.getStderr());
			stdErrString = processStream(stdErr, CHARSET);
			if (StringUtils.isNotEmpty(stdErrString)) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("命令执行错误信息:" + stdErrString);
				}
			}
			
			while((line = reader.readLine()) != null) {
				line = line.trim();
				if (StringUtils.isEmpty(line)) {
					continue;
				}
				lineList.add(line);
			}
		} catch (Exception e) {
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("执行linux命令报错,line:"+line, e);
			}
		} finally {
			if (null != conn) {
				conn.close();
			}
			if (null != reader) {
				try {
					reader.close();
				} catch (IOException e) {
					LOGGER.error("BufferedReader关闭异常！", e);
				}
			}
		}
		return lineList;
	}

	/**
	 * @param remoteFile	远程服务器文件
	 * @param localPath		本地目录
	 */
	public boolean copyRemoteFile(String remoteFile,String localPath)  {
		if (!login()) {
			LOGGER.info("远程服务器用户名或密码不正确!");
			return false;
		}
		try {
			client = conn.createSCPClient();
			client.get(remoteFile, localPath);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean putRemoteFile(String localFile,String remotePath)  {
		if (!login()) {
			LOGGER.info("远程服务器用户名或密码不正确!");
			return false;
		}
		try {
			client = conn.createSCPClient();
			client.put(localFile,remotePath );
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private String processStream(InputStream in, String charset) throws IOException {
		byte[] buf = new byte[1024];
		StringBuilder sb = new StringBuilder();
		while (in.read(buf) != -1) {
			sb.append(new String(buf, charset));
		}
		return sb.toString();
	}
	
	public static void main(String[] args)  {
		RemoteExecuteCommand command = new RemoteExecuteCommand("192.168.1.201","root","beacon");
		command.execute("scp root@192.168.1.143:/run/metadata/xml/aa.txt /run/metadata/xml/");
	}

	static final Logger LOGGER = LoggerFactory.getLogger(RemoteExecuteCommand.class);
}
