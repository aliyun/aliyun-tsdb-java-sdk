package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;

public class TagsTest {
    @Test
    public void testTagsOperationSerialization() {
        {
            TagsAddInfo tagsAddInfo = new TagsAddInfo.Builder("test").id("2").tag("tagk1", "tagv1").build();
            String serializedString = JSON.toJSONString(tagsAddInfo);
            Assert.assertEquals(serializedString, "{\"_ids\":[\"2\"],\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\"}}");
        }

        {
            TagsAddInfo tagsAddInfo = new TagsAddInfo.Builder("test").id("2").id("3")
                    .tag("tagk1", "tagv1").tag("tagk2", "tagv2").build();
            String serializedString = JSON.toJSONString(tagsAddInfo);
            Assert.assertEquals(serializedString, "{\"_ids\":[\"2\",\"3\"],\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\"}}");
        }

        {
            TagsShowInfo tagsShowInfo = new TagsShowInfo("test","2");
            String serializedString = JSON.toJSONString(tagsShowInfo);
            Assert.assertEquals(serializedString, "{\"_id\":\"2\",\"metric\":\"test\"}");
        }

        {
            TagsRemoveInfo tagsRemoveInfo = new TagsRemoveInfo.Builder("test","2").tag("tagk1", "tagv1").build();
            String serializedString = JSON.toJSONString(tagsRemoveInfo);
            Assert.assertEquals(serializedString, "{\"_id\":\"2\",\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\"}}");
        }

        {
            TagsRemoveInfo tagsRemoveInfo = new TagsRemoveInfo.Builder("test","2")
                    .tag("tagk1", "tagv1").tag("tagk2", "tagv2").build();
            String serializedString = JSON.toJSONString(tagsRemoveInfo);
            Assert.assertEquals(serializedString, "{\"_id\":\"2\",\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\"}}");
        }
    }
}
