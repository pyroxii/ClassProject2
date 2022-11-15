/* NOTICE: All materials provided by this project, and materials derived 
 * from the project, are the property of the University of Texas. 
 * Project materials, or those derived from the materials, cannot be placed 
 * into publicly accessible locations on the web. Project materials cannot 
 * be shared with other project teams. Making project materials publicly 
 * accessible, or sharing with other project teams will result in the 
 * failure of the team responsible and any team that uses the shared materials. 
 * Sharing project materials or using shared materials will also result 
 * in the reporting of all team members for academic dishonesty. 
 */ 
 
package utd.persistentDataStore.datastoreClient;

import java.net.InetAddress;
import java.util.List;

public class DatastoreClientImpl implements DatastoreClient
{
	//private static Logger logger = Logger.getLogger(DatastoreClientImpl.class);
	
	private InetAddress address;
	private int port;

	public DatastoreClientImpl(InetAddress address, int port)
	{
		this.address = address;
		this.port = port;
	}

	/* (non-Javadoc)
	 * @see utd.persistentDataStore.datastoreClient.DatastoreClient#write(java.lang.String, byte[])
	 */
	
	@Override
    public void write(String name, byte data[]) throws ClientException, ConnectionException
	{
		// Replace with implementation
		
		logger.debug("Executing Write Operation");
		
		//String feedback = null;
		
		try {
				@SuppressWarnings("resource")
				Socket clientSocket = new Socket(address, port);
				
				InputStream clientSocInput = clientSocket.getInputStream();
				OutputStream clientSocOutput = clientSocket.getOutputStream();
				
				StreamUtil.writeLine("write", clientSocOutput); 
				StreamUtil.writeLine(name, clientSocOutput);
				StreamUtil.writeLine(Integer.toString(data.length), clientSocOutput);
				StreamUtil.writeData(data, clientSocOutput);
				
				
				 logger.debug("Reading Response");
				 String feedback = StreamUtil.readLine(clientSocInput);
				 logger.debug("Response Code" + feedback);
			
		}
		catch (IOException e) 
		{
			throw new ClientException("Executing Write Operation", e);		
		}
		
	}	

	/* (non-Javadoc)
	 * @see utd.persistentDataStore.datastoreClient.DatastoreClient#read(java.lang.String)
	 */
	
	@Override
    public byte[] read(String name) throws ClientException, ConnectionException
	{
		byte[] data = null ;
		logger.debug("Executing Read Operation");
		
		try {
				
				Socket clientSocket = new Socket(address, port);
				
				InputStream clientSocInput = clientSocket.getInputStream();
				OutputStream clientSocOutput = clientSocket.getOutputStream();
				
				logger.debug("sending Request");
				StreamUtil.writeLine("read", clientSocOutput); 
				StreamUtil.writeLine(name, clientSocOutput);
				
				logger.debug("Reading Response");
				String feedback = StreamUtil.readLine(clientSocInput);
				
				if (!feedback.equalsIgnoreCase("ok")){				
					clientSocInput.close();
					clientSocOutput.close();
					clientSocket.close();
					throw new ClientException(feedback);
				} else {
					logger.debug("Read success");
					int datalength = Integer.parseInt(StreamUtil.readLine(clientSocInput));
					data = StreamUtil.readData(datalength, clientSocInput);
									
				}
				
				clientSocInput.close();
				clientSocOutput.close();
				clientSocket.close();
				
			} catch (Exception e){
				throw new ClientException("Read Error: No file name "+ name + ":\n",e);
			}
			return data;
	}			
				
				
				
	@Override
    public void delete(String name) throws ClientException, ConnectionException
	{
		logger.debug("Executing Delete Operation");
		
		try {
			
			Socket clientSocket = new Socket(address, port);
			
			InputStream clientSocInput = clientSocket.getInputStream();
			OutputStream clientSocOutput = clientSocket.getOutputStream();
			
			logger.debug("sending Request");
			StreamUtil.writeLine("delete", clientSocOutput); 
			StreamUtil.writeLine(name, clientSocOutput);
			
			logger.debug("Reading Response");
			String feedback = StreamUtil.readLine(clientSocInput);
			
			//if feedback fail then throw error 
			if (!feedback.equalsIgnoreCase("ok")){				
				clientSocInput.close();
				clientSocOutput.close();
				clientSocket.close();
				throw new ClientException( feedback);
			} else {
				logger.debug("delete success");								
			}
			
			clientSocInput.close();
			clientSocOutput.close();
			clientSocket.close();
			
		} catch (Exception e){
			throw new ClientException("Delete Error: No file name "+ name + ":\n",e);
		}
			
	}
	
			
	/* (non-Javadoc)
	 * @see utd.persistentDataStore.datastoreClient.DatastoreClient#directory()
	 */
	@Override
    public List<String> directory() throws ClientException, ConnectionException
	{
		logger.debug("Executing Directory Operation");
		List<String> filelist = new ArrayList<>();
		String feedback;
		
		try{
			//connection built
			Socket clientSoc = new Socket(address, port); 		
			
			//set input and output Stream
			InputStream clientSocInput = clientSoc.getInputStream();		
			OutputStream clientSocOutput  = clientSoc.getOutputStream();

			//send command to server
			StreamUtil.writeLine("directory", clientSocOutput);
			
			//get feedback from server
			feedback=StreamUtil.readLine(clientSocInput);
			
			//if feedback fail then throw error else get data
			if (!feedback.equalsIgnoreCase("ok")){				
				clientSocInput.close();
				clientSocOutput.close();
				clientSoc.close();
				throw new ClientException(feedback);
			} else {
				logger.debug("directory success");		
				int datalength = Integer.parseInt(StreamUtil.readLine(clientSocInput));
				for(int i=0;i<datalength;i++){
					filelist.add(StreamUtil.readLine(clientSocInput));
				}
			}
			
			clientSocInput.close();
			clientSocOutput.close();
			clientSoc.close();
			
		} catch (Exception e){
			throw new ClientException("directory Error ",e);
		}
		
		
		return filelist;
	}
	
}
