package kafka.playground;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class CpuUsageService {

    private final CentralProcessor centralProcessorRef;


    public CpuUsageService() {
        centralProcessorRef = new SystemInfo().getHardware().getProcessor();
    }


    /**
     * returns cpu usage as a double from 0 to 1
     */
    public double readCpuUsage(long waitBeforeReturning) {
        return centralProcessorRef.getSystemCpuLoad(waitBeforeReturning) * 100;
    }

    /**
     * returns cpu usage as a double from 0 to 1 with a delay of 1 second
     */
    public double readCpuUsage() {
        return centralProcessorRef.getSystemCpuLoad(1000);
    }
}
