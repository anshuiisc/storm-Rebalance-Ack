package org.apache.storm.starter.storm.genevents;

import java.util.List;

public interface ISyntheticEventGen {
	public void receive(List<String> event);  //event
}
