package dsparq.partitioning;

import java.util.List;

import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;

public class PropertyFileTester {

	public static void main(String[] args) {
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		List<HostInfo> shardsHostInfo = propertyFileHandler.getAllShardsInfo();
		for(HostInfo hinfo : shardsHostInfo)
			System.out.println(hinfo.getHost() + " " + hinfo.getPort());
	}
}
