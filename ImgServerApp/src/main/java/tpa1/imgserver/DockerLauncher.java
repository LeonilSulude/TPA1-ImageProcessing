package tpa1.imgserver;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;

public class DockerLauncher {
    private final DockerClient dockerClient;

    public DockerLauncher() {
        String dockerHost = System.getProperty("dockerHost", "unix:/var/run/docker.sock"); // corrigido, sem '///'

        var config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(dockerHost)
                .build();

        DockerHttpClient http = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .build();

        this.dockerClient = DockerClientImpl.getInstance(config, http);
    }


    public String launchResize(String imageName, String hostVolumePath, String inRel, String outRel, double pct){
        try {
            Volume vol = new Volume("/images");
            HostConfig hc = HostConfig.newHostConfig().withBinds(new Bind(hostVolumePath, vol));
            String[] cmd = new String[]{ "/images/input/"+inRel, "/images/output/"+outRel, String.valueOf(pct)};
            CreateContainerResponse c = dockerClient.createContainerCmd(imageName)
                    .withHostConfig(hc)
                    .withCmd(cmd)
                    .exec();
            dockerClient.startContainerCmd(c.getId()).exec();
            return c.getId();
        } catch(Exception e){ throw new RuntimeException(e); }
    }

    public boolean isRunning(String containerId){
        InspectContainerResponse st = dockerClient.inspectContainerCmd(containerId).exec();
        return st.getState().getRunning();
    }

    public void remove(String containerId){
        try { dockerClient.removeContainerCmd(containerId).withForce(true).exec(); } catch(Exception ignore) {}
    }
}