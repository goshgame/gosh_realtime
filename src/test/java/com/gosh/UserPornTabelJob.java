package com.gosh;

import com.gosh.job.UserFeatureCommon;
import com.gosh.job.UserPornLabelJob;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.*;

import static com.gosh.job.UserPornLabelJob.getPornLabel;

public class UserPornTabelJob
{
    public static void main(String[] args) throws Exception {


        List<Tuple4<List<UserFeatureCommon.PostViewInfo>, Long, Integer, String>> exposures = new ArrayList<>();
        List<Integer> positiveActions = Arrays.asList(1,3,5,6); // 点赞，评论，分享，收藏

        List<String> labels = Arrays.asList("1", "2", "3", "4");
        for (int i =0; i < 10; i++) {
            UserFeatureCommon.PostViewInfo info = new UserFeatureCommon.PostViewInfo();
            double random = Math.random();
            info.standingTime = (float) (i * random);
            if (i == 3) {
                info.interaction = List.of(1);
            }
            List<UserFeatureCommon.PostViewInfo> list = new ArrayList<>();
            list.add(info);
            Tuple4<List<UserFeatureCommon.PostViewInfo>, Long, Integer, String> tp = new Tuple4<>(list, 123L, i, labels.get(i % labels.size()));
            exposures.add(tp);
        }

        UserPornLabelJob.UserNExposures event = new UserPornLabelJob.UserNExposures(123, exposures);
        String label = getPornLabel(event, positiveActions);
        System.out.println(label);
    }
}
